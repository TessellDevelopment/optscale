#!/usr/bin/env python
import csv
import gc
import logging
import math
import os
import re
import pyarrow
import zipfile
import json
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta, timezone
from functools import cached_property

from diworker.diworker.importers.base import (
    CSVBaseReportImporter, CSV_REWRITE_DAYS
)
from diworker.diworker.cache.aws_report_cache import AWSReportCache
import tools.optscale_time as opttime
import pyarrow.parquet as pq

LOG = logging.getLogger(__name__)
# Chunk size for CSV processing - balanced for performance and memory
# With memory stable at ~1GB, we can use larger chunks for better performance
# Larger chunks = fewer MongoDB writes = faster processing
DEFAULT_CHUNK_SIZE = 500  # Optimized for speed now that memory is under control
CHUNK_SIZE = int(os.environ.get('AWS_CSV_CHUNK_SIZE', DEFAULT_CHUNK_SIZE))
# Configurable batch size for Parquet processing
AWS_REPORT_BATCH_SIZE = int(os.environ.get('AWS_REPORT_BATCH_SIZE', 2000))
IGNORE_EXPENSE_TYPES = ['Credit']
RI_PLATFORMS = [
    'Linux/UNIX',
    'Linux with SQL Server Standard',
    'Linux with SQL Server Web',
    'Linux with SQL Server Enterprise',
    'SUSE Linux',
    'Red Hat Enterprise Linux',
    'Red Hat Enterprise Linux with HA',
    'Windows',
    'Windows with SQL Server Standard',
    'Windows with SQL Server Web',
    'Windows with SQL Server Enterprise',
]
SERVICE_TAGS_MAP = {
    'user_name': 'user:Name',
    'aws_cloudformation_stack_id': 'aws:cloudformation:stack-id',
    'aws_cloudformation_logical_id': 'aws:cloudformation:logical-id',
    'aws_cloudformation_stack_name': 'aws:cloudformation:stack-name',
    'aws_created_by': 'aws:createdBy',
}
SERVICE_TAG_PREFIXES = ['aws', 'user', 'cloudformation']
# This map is needed for proper extraction of nested objects
# format: {field_name_prefix: (is_lowercase, [case exceptions])}
AWS_CUR_PREFIX_MAP = {
    'identity': (False, []),
    'bill': (False, []),
    'discount': (False, []),
    'line_item': (False, []),
    'product': (True, ['product_name', 'purchase_option', 'size_flex']),
    'pricing': (True, ['rate_code', 'rate_id', 'purchase_option',
                       'offering_class', 'lease_contract_length']),
    'reservation': (False, []),
    'savings_plan': (False, []),
    'resource_tags': (False, []),
    'cost_category': (False, []),
}
EDP_DISCOUNTS = ['discount/EdpDiscount', 'discount/PrivateRateDiscount']


class AWSReportImporter(CSVBaseReportImporter):
    ITEM_TYPE_ID_FIELDS = {
        'Tax': ['lineItem/TaxType', 'product/ProductName'],
        'Usage': ['lineItem/ProductCode', 'lineItem/Operation',
                  'product/region'],
    }
    STORAGE_LENS_TYPE = 'StorageLens'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.main_resources_product_family_map = {
            'Bucket': ['Storage', 'Data Transfer', 'Fee'],
            'Instance': ['Compute Instance', 'Stopped Instance'],
            'Snapshot': ['Storage Snapshot'],
            'Volume': ['Storage'],
            'Savings Plan': [],
            'Reserved Instances': [],
            'Load Balancer': []
        }
        self.import_start_ts = int(opttime.utcnow().timestamp())
        self.current_billing_period = None
        # Initialize AWS Report Cache
        self._report_cache = None
        self.cache_stats = {'hits': 0, 'misses': 0}
        # Cache for column name mappings to avoid repeated conversions
        self._legacy_column_cache = {}
        # Map of file paths to their cache keys for symlink lookup
        self._file_cache_keys = {}

    @property
    def report_cache(self):
        """Lazy initialization of report cache."""
        if self._report_cache is None:
            self._report_cache = AWSReportCache()
            LOG.info(f"Initialized AWS Report Cache at {self._report_cache.cache_dir}")
        return self._report_cache

    @cached_property
    def strip_edp(self):
        result = self.config_cl.get("/force_aws_edp_strip").value
        return result if result == "True" else False

    @cached_property
    def use_edp_discount(self):
        return self.cloud_acc['config'].get('use_edp_discount', False)

    @staticmethod
    def short_resource_id(resource_id):
        return resource_id[resource_id.find('/') + 1:]

    @staticmethod
    def unzip_report(report_path, dest_dir):
        LOG.info('Extracting %s as zip archive to %s', report_path, dest_dir)
        if zipfile.is_zipfile(report_path):
            with zipfile.ZipFile(report_path, 'r') as f_zip:
                if len(f_zip.filelist) > 1:
                    raise Exception('zip excepted to have one file inside')
                f_zip.extractall(dest_dir)
                new_report_path = os.path.join(
                    dest_dir, f_zip.filelist[0].filename)

            return new_report_path

    @staticmethod
    def to_camel_case(snake_str):
        return "".join(x.capitalize() for x in snake_str.split("_"))

    @staticmethod
    def to_lower_case(snake_str):
        return snake_str[0].lower() + snake_str[1:]

    def unpack_report(self, report_file, date):
        dest_dir = self.get_new_report_path(date)
        os.makedirs(dest_dir, exist_ok=True)
        if zipfile.is_zipfile(report_file):
            new_report_path = self.unzip_report(report_file, dest_dir)
        else:
            new_report_path = self.gunzip_report(report_file, dest_dir)
        if new_report_path:
            os.remove(report_file)
            return new_report_path
        else:
            return report_file

    def unpack_report_files(self):
        """Override to use symlinks for uncompressed files."""
        for date, reports in self.report_files.items():
            unpacked_reports = []
            for report_file in reports:
                # Try to use cached uncompressed version
                unpacked_path = self._unpack_with_cache(report_file, date)
                unpacked_reports.append(unpacked_path)
            self.report_files[date] = unpacked_reports

    def _unpack_with_cache(self, report_file, date):
        """
        Unpack a report file using cache if available, with symlink support.
        Uses locking to ensure only one thread unpacks each unique file.

        Args:
            report_file: Path to compressed report file
            date: Date string for organizing reports

        Returns:
            Path to uncompressed file (may be symlink)
        """
        # Get cache key from the mapping created during download
        cache_key = self._file_cache_keys.get(report_file)

        if not cache_key:
            # Fallback if cache key not found (shouldn't happen normally)
            LOG.warning(f"Cache key not found for {report_file}, will unpack without cache")
            dest_dir = self.get_new_report_path(date)
            os.makedirs(dest_dir, exist_ok=True)
            if zipfile.is_zipfile(report_file):
                new_report_path = self.unzip_report(report_file, dest_dir)
            else:
                new_report_path = self.gunzip_report(report_file, dest_dir)
            if new_report_path:
                os.remove(report_file)
                return new_report_path
            else:
                return report_file

        # Determine expected uncompressed extension
        uncompressed_ext = ''
        base_name = os.path.basename(report_file)
        if '.csv' in base_name or '.parquet' in base_name:
            if '.csv' in base_name:
                uncompressed_ext = '.csv'
            else:
                uncompressed_ext = '.parquet'

        dest_dir = self.get_new_report_path(date)
        os.makedirs(dest_dir, exist_ok=True)

        # ALWAYS acquire lock for unpacking to ensure thread safety
        # This prevents multiple threads from unpacking the same file simultaneously
        lock_path = self.report_cache._get_lock_path(f"{cache_key}_unpack")
        lock_file = self.report_cache._acquire_lock(lock_path)

        if not lock_file:
            # Lock timeout - proceed without cache as fallback
            LOG.warning(f"Lock timeout for unpacking {report_file}, proceeding without cache")
            if zipfile.is_zipfile(report_file):
                new_report_path = self.unzip_report(report_file, dest_dir)
            else:
                new_report_path = self.gunzip_report(report_file, dest_dir)
            if new_report_path:
                os.remove(report_file)
                return new_report_path
            return report_file

        try:
            # Check if uncompressed version exists in cache NOW (after acquiring lock)
            cached_uncompressed = self.report_cache.get_cached_uncompressed_file(
                cache_key, uncompressed_ext)

            if cached_uncompressed:
                # Another thread already unpacked it! Use that.
                LOG.info(f"File already unpacked by another thread, using cache: {cache_key}")
                link_path = os.path.join(dest_dir, os.path.basename(cached_uncompressed))
                if self.report_cache.create_symlink(cached_uncompressed, link_path):
                    self.cache_stats['hits'] += 1
                    LOG.info(f"Using cached uncompressed file via symlink: {link_path}")
                    # Remove the compressed file
                    if os.path.exists(report_file):
                        os.remove(report_file)
                    return link_path
                else:
                    # Symlink failed, fall back to copying cached file
                    LOG.warning(f"Symlink creation failed, copying from cache instead")
                    import shutil
                    copy_path = os.path.join(dest_dir, os.path.basename(cached_uncompressed))
                    shutil.copy2(cached_uncompressed, copy_path)
                    if os.path.exists(report_file):
                        os.remove(report_file)
                    return copy_path

            # We are the first to unpack this file - do the work IN CACHE
            self.cache_stats['misses'] += 1
            LOG.info(f"Unpacking file to cache: {cache_key}")

            # Get the compressed file path (should be in cache or a symlink to cache)
            # Resolve symlink to get actual cached compressed file
            if os.path.islink(report_file):
                compressed_source = os.readlink(report_file)
            else:
                compressed_source = report_file

            # Create a temporary directory IN CACHE for unpacking
            import tempfile
            cache_temp_dir = tempfile.mkdtemp(
                prefix='unpack_',
                dir=self.report_cache.uncompressed_dir)

            try:
                # Unpack directly in cache temp directory
                # Need to check the actual file, not symlink, for zip detection
                actual_file = compressed_source if not os.path.islink(compressed_source) else os.path.realpath(compressed_source)

                LOG.info(f"Unpacking file: {actual_file} (original: {compressed_source})")

                if zipfile.is_zipfile(actual_file):
                    temp_unpack_path = self.unzip_report(actual_file, cache_temp_dir)
                else:
                    temp_unpack_path = self.gunzip_report(actual_file, cache_temp_dir)

                if not temp_unpack_path:
                    LOG.error(f"Failed to unpack file: {actual_file}")
                    raise Exception(f"Unpack failed for {actual_file}")

                if temp_unpack_path:
                    # Move to final cache location atomically
                    try:
                        final_cache_path = self.report_cache._get_uncompressed_path(
                            cache_key, uncompressed_ext)

                        # Atomic move within cache
                        import shutil
                        shutil.move(temp_unpack_path, final_cache_path)
                        LOG.info(f"Stored uncompressed file in cache: {final_cache_path}")

                        # Now create symlink in datasource folder to the cached file
                        link_path = os.path.join(dest_dir, os.path.basename(final_cache_path))
                        if self.report_cache.create_symlink(final_cache_path, link_path):
                            LOG.info(f"Created symlink to cached file: {link_path} -> {final_cache_path}")
                            final_path = link_path
                        else:
                            # Symlink failed, copy as fallback
                            LOG.warning(f"Symlink creation failed, copying from cache")
                            copy_path = os.path.join(dest_dir, os.path.basename(final_cache_path))
                            shutil.copy2(final_cache_path, copy_path)
                            final_path = copy_path

                    except Exception as e:
                        LOG.error(f"Failed to cache uncompressed file: {e}")
                        # Fallback: move from temp to datasource folder
                        final_path = os.path.join(dest_dir, os.path.basename(temp_unpack_path))
                        import shutil
                        shutil.move(temp_unpack_path, final_path)

                    # Remove compressed symlink/file from datasource folder
                    if os.path.exists(report_file):
                        os.remove(report_file)

                    # Clean up temp directory
                    try:
                        import shutil
                        shutil.rmtree(cache_temp_dir, ignore_errors=True)
                    except Exception:
                        pass

                    return final_path
                else:
                    # Unpacking failed, clean up temp dir and return original
                    import shutil
                    shutil.rmtree(cache_temp_dir, ignore_errors=True)
                    return report_file

            except Exception as e:
                LOG.error(f"Error during unpacking: {e}")
                # Clean up temp directory
                import shutil
                shutil.rmtree(cache_temp_dir, ignore_errors=True)
                # Return the compressed file (symlink or copy)
                return report_file

        finally:
            # Always release the lock
            self.report_cache._release_lock(lock_file)

    def _download_report_files(self, current_reports, last_import_modified_at):
        """
        Override to use cache for downloads.
        Downloads ONLY to cache, creates symlinks in datasource folders.
        """
        config = self.cloud_acc.get('config', {})
        bucket_name = config.get('bucket_name', '')
        bucket_prefix = config.get('bucket_prefix', '')
        report_name = config.get('report_name', '')

        for date, reports in current_reports.items():
            # Create datasource directory structure for symlinks
            datasource_dir = os.path.join(self.reports_dir, date)
            os.makedirs(datasource_dir, exist_ok=True)

            for report in reports:
                if last_import_modified_at < report['LastModified']:
                    last_import_modified_at = report['LastModified']

                s3_key = report['Key']

                # Determine file extension
                file_extension = ''
                if s3_key.endswith('.gz'):
                    file_extension = '.gz'
                elif s3_key.endswith('.zip'):
                    file_extension = '.zip'

                # Generate cache key
                cache_key = self.report_cache._generate_cache_key(
                    bucket_name, bucket_prefix, report_name, s3_key)

                # Get cache path for compressed file
                cached_compressed_path = self.report_cache._get_compressed_path(
                    cache_key, file_extension)

                # Download function that writes DIRECTLY to cache
                def download():
                    try:
                        # python2 way
                        with open(cached_compressed_path, 'wb') as f_report:
                            self.cloud_adapter.download_report_file(s3_key, f_report)
                    except TypeError:
                        # python3 way
                        with open(cached_compressed_path, 'w') as f_report:
                            self.cloud_adapter.download_report_file(s3_key, f_report)

                # Use cache with locking (downloads directly to cache)
                try:
                    self.report_cache.get_or_download_compressed_file(
                        download, cached_compressed_path, bucket_name, bucket_prefix,
                        report_name, s3_key, report['LastModified'], file_extension)

                    # Create symlink in datasource folder pointing to cache
                    symlink_name = os.path.basename(s3_key)
                    symlink_path = os.path.join(datasource_dir, symlink_name)

                    if self.report_cache.create_symlink(cached_compressed_path, symlink_path):
                        LOG.info(f"Created symlink for compressed file: {symlink_path} -> {cached_compressed_path}")
                    else:
                        # Symlink failed, copy as fallback
                        LOG.warning(f"Symlink failed, copying compressed file instead")
                        import shutil
                        shutil.copy2(cached_compressed_path, symlink_path)

                    # Store symlink path for later processing
                    self.report_files[date].append(symlink_path)

                    # Store cache key mapping for unpack phase
                    self._file_cache_keys[symlink_path] = cache_key

                except Exception as e:
                    LOG.error(f"Error downloading with cache: {e}. Falling back to direct download.")
                    # Fallback: download to datasource folder directly
                    fallback_path = os.path.join(datasource_dir, os.path.basename(s3_key))
                    try:
                        with open(fallback_path, 'wb') as f_report:
                            self.cloud_adapter.download_report_file(s3_key, f_report)
                    except TypeError:
                        with open(fallback_path, 'w') as f_report:
                            self.cloud_adapter.download_report_file(s3_key, f_report)
                    self.report_files[date].append(fallback_path)
                    self._file_cache_keys[fallback_path] = cache_key

        return last_import_modified_at

    @staticmethod
    def get_unique_field_list(include_date=True):
        # todo: if this is not enough, we may lose data on raw import
        # we need functional testing. at least verify count of records after
        # import. also total sum from CSV must match total sum for clean records
        unique_list = [
            'lineItem/LineItemDescription',
            'lineItem/LineItemType',
            'lineItem/UsageType',
            'lineItem/Operation',
            'lineItem/ProductCode',
            'lineItem/ResourceId',
            'cloud_account_id',
            'lineItem/AvailabilityZone',
            'savingsPlan/SavingsPlanARN',
            'reservation/ReservationARN'
            'bill/BillingPeriodStartDate',
            'resource_id'
        ]
        if include_date:
            unique_list.extend([
                'lineItem/UsageStartDate',
                'start_date'
            ])
        return unique_list

    def get_update_fields(self):
        return [
            'discount/EdpDiscount',
            'lineItem/BlendedRate',
            'lineItem/BlendedCost',
            'lineItem/UnblendedRate',
            'lineItem/UnblendedCost',
            'lineItem/UsageEndDate',
            'lineItem/UsageAmount',
            'savingsPlan/SavingsPlanEffectiveCost',
            'reservation/EffectiveCost',
            'pricing/publicOnDemandCost',
            'savingPlan/UsedCommitment',
            'savingsPlan/SavingsPlanRate',
            'reservation/UnusedQuantity',
            'reservation/UnusedRecurringFee',
            'reservation/UnusedAmortizedUpfrontFeeForBillingPeriod',
            'reservation/AmortizedUpfrontFeeForBillingPeriod',
            'end_date',
            'cost',
            'report_identity',
            '_rec_n'
        ]

    @staticmethod
    def _is_first_import_in_month(last_import_dt: datetime):
        now = opttime.utcnow()
        if (last_import_dt.month + 1 == now.month and
                last_import_dt.year == now.year) or (
                    now.month == 1 and last_import_dt.year + 1 == now.year):
            return True

    def get_current_reports(self, reports_groups, last_import_modified_at):
        current_reports = defaultdict(list)
        reports_count = 0
        # during first report in the current month download all reports
        # from the previous month to do full reimport
        if self._is_first_import_in_month(last_import_modified_at):
            last_import_modified_at = opttime.startmonth(
                last_import_modified_at)
        for date, reports in reports_groups.items():
            for report in reports:
                if report.get('LastModified', -1) > last_import_modified_at:
                    # use all reports for month
                    current_reports[date].extend(reports)
                    reports_count += len(reports)
                    break
        LOG.info('Selected %s reports', reports_count)
        return current_reports

    @cached_property
    def min_date_import_threshold(self) -> datetime:
        last_import_dt = datetime.fromtimestamp(
            self.cloud_acc.get('last_import_modified_at', 0), tz=timezone.utc)
        last_import_dt = last_import_dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if self._is_first_import_in_month(last_import_dt):
            # import full previous month on the first import in month
            return last_import_dt.replace(day=1)
        return last_import_dt - timedelta(days=CSV_REWRITE_DAYS)

    def get_raw_upsert_filters(self, expense):
        filters = super().get_raw_upsert_filters(expense)
        filters.update({
            '$or': [
                {'report_identity': {'$ne': self.report_identity}},
                {'_rec_n': expense['_rec_n']}
            ]
        })
        return filters

    def get_linked_account_map(self):
        org_id = self.cloud_acc['organization_id']
        _, linked_accs = self.rest_cl.cloud_account_list(
            org_id, only_linked=True, auto_import=True, type='aws_cnr')
        account_id_ca_id_map = {x['account_id']: x['id']
                                for x in linked_accs['cloud_accounts']}
        account_id_ca_id_map[self.cloud_acc['account_id']] = self.cloud_acc_id
        return account_id_ca_id_map

    def load_raw_data(self):
        account_id_ca_id_map = self.get_linked_account_map()
        report_files = []
        for r in self.report_files.values():
            report_files.extend(r)
        for report_path in report_files:
            self.load_report(report_path, account_id_ca_id_map)
        self.clear_rudiments()

    def _log_memory_usage(self, context=""):
        """Log current memory usage for monitoring."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / (1024 * 1024)
            LOG.info(f"Memory usage {context}: {mem_mb:.2f} MB (RSS)")
        except ImportError:
            # psutil not available, use resource module fallback
            try:
                import resource
                usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                # macOS reports in bytes, Linux in KB
                import platform
                if platform.system() == 'Darwin':
                    usage_mb = usage_kb / (1024 * 1024)
                else:
                    usage_mb = usage_kb / 1024
                LOG.info(f"Memory usage {context}: {usage_mb:.2f} MB (maxrss)")
            except Exception as e:
                # Even resource failed, at least log that we tried
                LOG.info(f"Memory monitoring {context}: psutil not available, GC active")
        except Exception as e:
            LOG.debug(f"Error logging memory: {e}")

    def load_report(self, report_path, account_id_ca_id_map):
        skipped_accounts = set()
        billing_period = None

        # Get file size for metrics
        file_size_mb = os.path.getsize(report_path) / (1024 * 1024)
        LOG.info(f'Loading report {report_path} (size: {file_size_mb:.2f} MB)')
        self._log_memory_usage("before loading report")

        start_time = opttime.utcnow()

        try:
            billing_period, skipped_accounts = self.load_parquet_report(
                report_path, account_id_ca_id_map, billing_period,
                skipped_accounts)
            file_type = 'Parquet'
        except pyarrow.lib.ArrowInvalid as exc:
            LOG.warning(
                f"Could not open source file as Parquet {report_path}: "
                f"{str(exc)}. Will try to open it as CSV")
            billing_period, skipped_accounts = self.load_csv_report(
                report_path, account_id_ca_id_map, billing_period,
                skipped_accounts)
            file_type = 'CSV'

        # Log performance metrics
        elapsed_time = (opttime.utcnow() - start_time).total_seconds()
        throughput_mbps = file_size_mb / elapsed_time if elapsed_time > 0 else 0
        LOG.info(f'Completed loading {file_type} report {report_path} in {elapsed_time:.2f}s '
                f'(throughput: {throughput_mbps:.2f} MB/s)')
        self._log_memory_usage("after loading report")

        # Force garbage collection after each report
        gc.collect()

        if billing_period:
            self.billing_periods.add(billing_period)
        if len(skipped_accounts) > 0:
            LOG.warning('Import skipped for following accounts: %s. Looks like '
                        'credentials for them weren\'t added or added '
                        'incorrectly or they are not marked as linked',
                        skipped_accounts)

    def update_raw_records(self, chunk):
        """
        Override to add aggressive memory management.
        MongoDB bulk operations hold massive buffers - must be cleaned immediately.
        """
        for row in chunk:
            # TODO: OS-5444
            # pymongo InsertOne fails on '.' in key, while UpdateOne splits
            # key by dot and saves as dict, ex.:
            #     {"resourceTags/user:test.dot": 1} ->
            #         {"resourceTags/user:test": {"dot": 1}}
            def split_dot_keys(key, value):
                result = {key: value}
                if '.' in key:
                    result.pop(key)
                    parts = key.split('.', 1)
                    result[parts[0]] = split_dot_keys(parts[1], value)
                    return result
                return result

            tags = {k: v for k, v in row.items() if
                    k.startswith('resourceTags')}
            for k, v in tags.items():
                row.pop(k)
                row.update(split_dot_keys(k, v))
            row['report_identity'] = self.report_identity

        # Write to MongoDB
        super().update_raw_records(chunk)

        # CRITICAL: Force MongoDB driver to flush buffers and release memory
        # The bulk_write operation leaves results and buffers in memory
        # We must explicitly clean up after EVERY write for large CSV files
        try:
            # Force connection to flush any pending operations
            self.mongo_raw.database.client.admin.command('ping')
        except Exception:
            pass  # Ignore ping errors, just trying to flush

        # Explicit garbage collection to clean MongoDB driver objects
        gc.collect(generation=0)

    @staticmethod
    def _is_flavor_usage(expense):
        usage_type = expense.get('lineItem/UsageType', '')
        service_code = expense.get('product/servicecode', '')
        description = expense.get('lineItem/LineItemDescription', '')
        item_type = expense.get('lineItem/LineItemType', '')
        return (
            (service_code == 'AmazonECS' and 'Fargate' in usage_type) or
            (service_code == 'AmazonSageMaker' and (
                'ml.' in description or item_type == 'SavingsPlanNegation')) or
            (service_code == 'AWSLambda' and 'Lambda-GB-Second' in usage_type) or
            ('BoxUsage' in usage_type)
        )

    def _set_resource_id(self, expense):
        item_res_id = expense.get('lineItem/ResourceId', '')
        if (expense.get('resource_id') is None or (
                # move SavingsPlanCoveredUsage expenses from applied
                # resource to 'Savings Plan' resource
                expense.get(
                    'lineItem/LineItemType') == 'SavingsPlanCoveredUsage' and
                expense.get('savingsPlan/SavingsPlanARN'))):
            res_id = self.compose_resource_id(expense)
            if res_id:
                expense['resource_id'] = res_id
        elif 'elasticloadbalancing:' in item_res_id:
            # classic load balancer resources may have the same value of name
            # in different regions, so use full resource_id
            expense['resource_id'] = item_res_id

    def _to_csv_tag(self, prefix, tag, root=True):
        if root and tag in SERVICE_TAGS_MAP:
            return f'{prefix}{SERVICE_TAGS_MAP[tag]}'
        subprefix = next((
            s for s in SERVICE_TAG_PREFIXES if tag.startswith(s)
        ), None)
        if not subprefix:
            return f'{prefix}{tag}'
        subkey = f'{tag[len(subprefix) + 1:]}'
        return self._to_csv_tag(f'{prefix}{subprefix}:', subkey, False)

    def _get_legacy_csv_key(self, old_key):
        """
        Convert snake_case CUR column names to legacy CSV format.
        Caches results to avoid redundant conversions.
        """
        # Check cache first
        if old_key in self._legacy_column_cache:
            return self._legacy_column_cache[old_key]

        # Perform conversion
        key = next((
            s for s in AWS_CUR_PREFIX_MAP.keys() if old_key.startswith(f'{s}_')
        ), None)
        if not key:
            result = old_key
        else:
            prefix = self.to_lower_case(self.to_camel_case(key))
            subkey = old_key[len(key) + 1:]
            if not subkey:
                result = prefix
            elif key == 'resource_tags':
                result = self._to_csv_tag(f'{prefix}/', subkey)
            else:
                to_lower, exceptions = AWS_CUR_PREFIX_MAP[key]
                new_key = self.to_camel_case(subkey)
                if subkey in exceptions:
                    to_lower = not to_lower
                if to_lower:
                    new_key = self.to_lower_case(new_key)
                result = f'{prefix}/{new_key}'

        # Cache the result
        self._legacy_column_cache[old_key] = result
        return result

    def _extract_nested_objects(self, obj, parquet=False):
        updates = defaultdict(dict)
        removed_keys = set()
        # extract nested objects
        for k in AWS_CUR_PREFIX_MAP.keys():
            values = obj.get(k)
            if not values:
                continue
            if parquet:
                for n, vals in values.items():
                    if isinstance(vals, list):
                        for postfix, value in vals:
                            snake_key = f'{k}_{postfix}'
                            csv_key = self._get_legacy_csv_key(snake_key)
                            updates[csv_key][n] = value
                        removed_keys.add(k)
            else:
                try:
                    nested_objects = json.loads(values)
                except Exception:
                    continue
                for new_key, new_value in nested_objects.items():
                    snake_key = f'{k}_{new_key}'
                    csv_key = self._get_legacy_csv_key(snake_key)
                    updates[csv_key] = new_value
                removed_keys.add(k)
        for k in removed_keys:
            obj.pop(k)
        obj.update(updates)

        # CRITICAL: Explicitly release memory for 5M+ rows
        del updates
        del removed_keys

        return obj

    def _convert_to_legacy_csv_columns(self, columns, dict_format=False):
        if not dict_format:
            return [self._get_legacy_csv_key(col) for col in columns]
        return {col: self._get_legacy_csv_key(col) for col in columns}

    def load_csv_report(self, report_path, account_id_ca_id_map,
                        billing_period, skipped_accounts):
        date_start = opttime.utcnow()

        # CRITICAL: Use minimal buffer AND advise kernel to not cache
        # For 1.2GB files with 18GB memory limit, file caching causes OOM risk
        BUFFER_SIZE = 1024  # 1KB buffer - minimal buffering for 1GB+ files

        # Resolve symlink to real file path (posix_fadvise doesn't work on symlinks)
        import os as os_module
        actual_file_path = os_module.path.realpath(report_path)

        # Open file with minimal caching hint to kernel
        fd = os_module.open(actual_file_path, os_module.O_RDONLY)

        # CRITICAL: Duplicate fd before fdopen() because fdopen() takes ownership
        # and will close the fd when the file object is closed
        # We need to keep a separate fd for posix_fadvise operations
        try:
            fd_for_fadvise = os_module.dup(fd)  # Create independent copy of fd
        except OSError as e:
            LOG.warning(f"Could not duplicate fd: {e}")
            fd_for_fadvise = None

        # Tell kernel: don't keep this file in page cache (prevents 28GB cache buildup)
        if fd_for_fadvise:
            try:
                # POSIX_FADV_SEQUENTIAL = 2 (optimize for sequential reading)
                # POSIX_FADV_DONTNEED = 4 (don't cache - drop pages immediately)
                # Note: POSIX_FADV_NOREUSE (5) is not supported on all filesystems
                POSIX_FADV_SEQUENTIAL = 2
                POSIX_FADV_DONTNEED = 4

                # Tell kernel this is sequential access (universally supported)
                os_module.posix_fadvise(fd_for_fadvise, 0, 0, POSIX_FADV_SEQUENTIAL)

                # Store the duplicated fd for later cache drops
                self._csv_fd_for_fadvise = fd_for_fadvise
                self._csv_filepath_for_fadvise = actual_file_path

                LOG.debug(f"Successfully set SEQUENTIAL hint for {actual_file_path}")
            except (AttributeError, OSError) as e:
                LOG.warning(f"Could not set file cache hints for {actual_file_path}: {e}")
                if fd_for_fadvise:
                    os_module.close(fd_for_fadvise)
                self._csv_fd_for_fadvise = None
        else:
            self._csv_fd_for_fadvise = None

        # Create file object from original fd (fdopen takes ownership and will close it)
        csvfile = os_module.fdopen(fd, mode='r', buffering=BUFFER_SIZE, newline='')

        with csvfile:
            reader = csv.DictReader(csvfile)
            reader.fieldnames = self._convert_to_legacy_csv_columns(
                reader.fieldnames)
            chunk = []
            record_number = 0
            for row in reader:
                row = self._extract_nested_objects(row)
                if billing_period is None:
                    billing_period = row['bill/BillingPeriodStartDate']
                    LOG.info('detected billing period: %s', billing_period)
                    self.current_billing_period = billing_period

                if len(chunk) >= CHUNK_SIZE:
                    self.update_raw_records(chunk)
                    # Explicitly delete and recreate chunk list to release memory
                    del chunk
                    chunk = []

                    # Force garbage collection periodically (every 10 chunks)
                    # With memory stable, we don't need GC after every chunk
                    # generation=0 is fastest and targets recently created objects
                    if record_number % (CHUNK_SIZE * 10) == 0:
                        gc.collect(generation=0)

                    # Drop file cache periodically to prevent OOM
                    # With memory stable at ~1GB, we can drop less frequently for better performance
                    # Drop every 10000 rows (~400MB) to minimize syscall overhead
                    if hasattr(self, '_csv_fd_for_fadvise') and self._csv_fd_for_fadvise and record_number % 10000 == 0:
                        try:
                            POSIX_FADV_DONTNEED = 4
                            import os as os_module
                            # Drop pages from 0 to current position
                            os_module.posix_fadvise(self._csv_fd_for_fadvise, 0, 0, POSIX_FADV_DONTNEED)
                            LOG.debug(f"Dropped cache at row {record_number}")
                        except Exception as e:
                            LOG.warning(f"Failed to drop cache at row {record_number}: {e}")

                    now = opttime.utcnow()
                    if (now - date_start).total_seconds() > 60:
                        LOG.info('report %s: processed %s rows',
                                 report_path, record_number)
                        date_start = now

                cloud_account_id = account_id_ca_id_map.get(
                    row['lineItem/UsageAccountId'])
                if cloud_account_id is None:
                    skipped_accounts.add(row['lineItem/UsageAccountId'])
                    continue

                self.detected_cloud_accounts.add(cloud_account_id)
                record_number += 1
                row['_rec_n'] = record_number
                row['cloud_account_id'] = cloud_account_id
                if 'lineItem/ResourceId' in row:
                    row['resource_id'] = self.short_resource_id(
                        row['lineItem/ResourceId'])
                start_date = self._datetime_from_expense(
                    row, 'lineItem/UsageStartDate').replace(
                    hour=0, minute=0, second=0)
                # RIFee is created once a month and is updated every day
                if (start_date < self.min_date_import_threshold and
                        row['lineItem/LineItemType'] != 'RIFee'):
                    continue
                row['start_date'] = start_date
                row['end_date'] = self._datetime_from_expense(
                    row, 'lineItem/UsageEndDate')
                row['cost'] = float(row['lineItem/BlendedCost']) if row[
                    'lineItem/BlendedCost'] else 0
                if self.use_edp_discount:
                    for field in EDP_DISCOUNTS:
                        row['cost'] += float(row.get(field) or 0)
                elif self.strip_edp:
                    for field in EDP_DISCOUNTS:
                        row.pop(field, None)
                if self._is_flavor_usage(row):
                    row['box_usage'] = True

                # CRITICAL: Don't use row.copy() - creates 5 million copies!
                # Instead, collect keys to delete first, then delete
                keys_to_delete = [k for k, v in row.items() if v == '']
                for k in keys_to_delete:
                    del row[k]
                del keys_to_delete  # Release list immediately

                self._set_resource_id(row)
                row['created_at'] = self.import_start_ts
                chunk.append(row)

            if chunk:
                self.update_raw_records(chunk)
                del chunk

        # CRITICAL: Drop entire file from cache after processing to prevent OOM
        # This releases 1.2GB of file cache immediately
        if hasattr(self, '_csv_fd_for_fadvise') and self._csv_fd_for_fadvise:
            try:
                import os as os_module
                POSIX_FADV_DONTNEED = 4
                # Tell kernel to drop ALL cached pages for this file
                os_module.posix_fadvise(self._csv_fd_for_fadvise, 0, 0, POSIX_FADV_DONTNEED)
                filepath = getattr(self, '_csv_filepath_for_fadvise', report_path)
                LOG.info(f"Dropped file cache for {filepath}")
            except Exception as e:
                LOG.warning(f"Could not drop file cache for {report_path}: {e}")
            finally:
                # CRITICAL: Close the duplicated fd to avoid fd leak
                if self._csv_fd_for_fadvise:
                    try:
                        import os as os_module
                        os_module.close(self._csv_fd_for_fadvise)
                    except Exception:
                        pass  # Ignore close errors
                self._csv_fd_for_fadvise = None

        # Final aggressive cleanup after processing CSV file
        # Full collection to clean up all generations
        gc.collect(generation=2)  # Full collection including old objects
        gc.collect(generation=2)  # Second pass for circular references
        return billing_period, skipped_accounts

    def load_parquet_report(self, report_path, account_id_ca_id_map,
                            billing_period, skipped_accounts):
        """
        Load Parquet report using streaming to minimize memory usage.
        Uses PyArrow's batch iterator instead of loading entire file into pandas.
        """
        date_start = opttime.utcnow()

        # Use streaming batch reader
        parquet_file = pq.ParquetFile(report_path)
        batch_size = AWS_REPORT_BATCH_SIZE

        LOG.info(f'Processing Parquet file with {parquet_file.metadata.num_rows} rows '
                f'in batches of {batch_size}')

        # Get column name mapping
        schema_columns = parquet_file.schema.names
        new_columns = self._convert_to_legacy_csv_columns(
            schema_columns, dict_format=True)

        row_offset = 0
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            # Convert batch to pandas dataframe (only one batch at a time in memory)
            batch_df = batch.to_pandas()
            batch_df.rename(columns=new_columns, inplace=True)

            # Process this batch
            expense_chunk = self._extract_nested_objects(
                batch_df.to_dict(), parquet=True)

            actual_size = len(batch_df)
            chunk = [{'cost': 0} for _ in range(actual_size)]
            skipped_rows = set()

            for field_name, values_dict in expense_chunk.items():
                for n, value in values_dict.items():
                    expense_num = n % actual_size
                    if expense_num in skipped_rows:
                        continue
                    chunk[expense_num]['_rec_n'] = row_offset + n
                    if hasattr(value, 'timestamp'):
                        value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
                    if (field_name == 'bill/BillingPeriodStartDate' and
                            billing_period is None):
                        billing_period = value
                        LOG.info('detected billing period: %s', billing_period)
                        self.current_billing_period = billing_period
                    elif field_name == 'lineItem/UsageAccountId':
                        cloud_account_id = account_id_ca_id_map.get(value)
                        if cloud_account_id is None:
                            skipped_accounts.add(value)
                            skipped_rows.add(expense_num)
                            continue
                        chunk[expense_num]['cloud_account_id'] = cloud_account_id
                        self.detected_cloud_accounts.add(cloud_account_id)
                    elif field_name == 'lineItem/ResourceId' and value:
                        chunk[expense_num][
                            'resource_id'] = self.short_resource_id(value)
                    elif field_name == 'lineItem/UsageStartDate':
                        start_date = self._datetime_from_value(value).replace(
                            hour=0, minute=0, second=0)
                        chunk[expense_num]['start_date'] = start_date
                    elif field_name == 'lineItem/UsageEndDate':
                        chunk[expense_num]['end_date'] = self._datetime_from_value(
                            value)
                    elif field_name == 'lineItem/BlendedCost':
                        chunk[expense_num]['cost'] = float(value) if value else 0
                    elif field_name in EDP_DISCOUNTS:
                        if self.use_edp_discount:
                            chunk[expense_num]['cost'] += float(value or 0)
                        elif self.strip_edp:
                            continue
                    elif field_name == 'lineItem/UsageType':
                        if value and 'BoxUsage' in value:
                            chunk[expense_num]['box_usage'] = True
                    if isinstance(value, float) and math.isnan(value):
                        value = 0
                    if value:
                        chunk[expense_num][field_name] = value

            expenses = [x for x in chunk
                        if chunk.index(x) not in skipped_rows and
                        x.get('cloud_account_id') is not None and
                        # RIFee is created once a month and is updated every day
                        (x.get('start_date') and x['start_date'] >= self.min_date_import_threshold or
                         x.get('lineItem/LineItemType') == 'RIFee')]
            for expense in expenses:
                expense['created_at'] = self.import_start_ts
                if self._is_flavor_usage(expense):
                    expense['box_usage'] = True
                self._set_resource_id(expense)
            if expenses:
                self.update_raw_records(expenses)
                now = opttime.utcnow()
                if (now - date_start).total_seconds() > 60:
                    LOG.info('report %s: processed %s rows', report_path, row_offset)
                    date_start = now

            # Update row offset for next batch
            row_offset += actual_size

            # Clear batch_df to free memory
            del batch_df
            del expense_chunk
            del chunk
            del expenses

            # Force garbage collection every 10 batches to reduce memory pressure
            if row_offset % (batch_size * 10) == 0:
                gc.collect()

        LOG.info(f'Completed processing Parquet file: {report_path}, total rows: {row_offset}')
        # Final garbage collection after processing file
        gc.collect()
        return billing_period, skipped_accounts

    def collect_tags(self, expense):
        raw_tags = {}

        def _extract_tag_name(tag_key, prefix_symbol):
            prefix_len = tag_key.find(prefix_symbol) + 1
            return tag_key[prefix_len:]

        for k, v in expense.items():
            if (not k.startswith('resourceTags') and
                    not k.startswith('resource_tags')):
                continue
            if k != 'resourceTags/user:Name':
                if k.startswith('resourceTags/aws'):
                    name = _extract_tag_name(k, '/')
                else:
                    name = _extract_tag_name(k, ':')
                raw_tags[name] = v
        tags = self.extract_tags(raw_tags)
        return tags

    @staticmethod
    def _datetime_from_expense(expense, key):
        value = expense[key]
        if isinstance(value, str):
            return AWSReportImporter._datetime_from_value(expense[key])
        return value.replace(tzinfo=timezone.utc)

    @staticmethod
    def _datetime_from_value(value):
        dt_format = '%Y-%m-%dT%H:%M:%SZ'
        if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z", value):
            dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        return datetime.strptime(value, dt_format).replace(tzinfo=timezone.utc)

    def get_resource_info_from_expenses(self, expenses, resource_type=None):
        name = None
        resource_type = resource_type
        region = None
        last_region = None
        service_name = None
        first_seen = datetime.now(tz=timezone.utc)
        tags = {}
        family_region_map = {}
        fake_cad_extras = {}
        meta_dict = {}
        last_seen = datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
        os_type = None
        preinstalled = None
        payment_option = None
        offering_type = None
        purchase_term = None
        applied_region = None
        start = None
        end = None
        instance_type = None
        platform = None
        zone = None

        for e in expenses:
            start_date = self._datetime_from_expense(
                e, 'start_date')
            if start_date and start_date < first_seen:
                first_seen = start_date
            end_date = self._datetime_from_expense(
                e, 'lineItem/UsageEndDate')
            if end_date and end_date > last_seen:
                last_seen = end_date

            product = e.get('lineItem/ProductCode')
            if product and any(k in product.lower() for k in ['aws', 'amazon']):
                service_name = product
            elif service_name is None:
                service_name = e.get('bill/BillingEntity')

            name = e.get('resourceTags/user:Name') or name
            product_family = e.get('product/productFamily')
            tags.update(self.collect_tags(e))
            operation = e.get('lineItem/Operation')
            if operation == self.STORAGE_LENS_TYPE:
                resource_type = self.STORAGE_LENS_TYPE
            elif resource_type not in self.main_resources_product_family_map.keys():
                resource_type = self.get_resource_type(e, resource_type)
            last_region = e.get('product/region')
            fake_cad_extras.update(self._get_fake_cad_extras(e))

            family_value_list = self.main_resources_product_family_map.get(
                resource_type, [])
            if family_value_list and last_region and product_family:
                for family in family_value_list:
                    if family in product_family:
                        family_region_map[family] = last_region
                        break

            if resource_type == 'Instance':
                if not os_type and 'product/operatingSystem' in e:
                    os_type = e['product/operatingSystem']
                    meta_dict['os'] = os_type
                if not preinstalled and 'product/preInstalledSw' in e:
                    preinstalled = e['product/preInstalledSw']
                    meta_dict['preinstalled'] = preinstalled
            elif resource_type == 'Load Balancer':
                name = self.short_resource_id(e['resource_id'])
            elif resource_type == 'Savings Plan':
                if not payment_option and 'savingsPlan/PaymentOption' in e:
                    payment_option = e['savingsPlan/PaymentOption']
                    meta_dict['payment_option'] = payment_option
                if not offering_type and 'savingsPlan/OfferingType' in e:
                    offering_type = e['savingsPlan/OfferingType']
                    meta_dict['offering_type'] = offering_type
                if not purchase_term and 'savingsPlan/PurchaseTerm' in e:
                    purchase_term = e['savingsPlan/PurchaseTerm']
                    meta_dict['purchase_term'] = purchase_term
                if not applied_region and 'savingsPlan/Region' in e:
                    applied_region = e['savingsPlan/Region']
                    meta_dict['applied_region'] = applied_region
                if not start and 'savingsPlan/StartTime' in e:
                    try:
                        start = int(self._datetime_from_value(
                            e['savingsPlan/StartTime']).timestamp())
                        meta_dict['start'] = start
                    except (TypeError, ValueError):
                        pass
                if not end and 'savingsPlan/EndTime' in e:
                    try:
                        end = int(self._datetime_from_value(
                            e['savingsPlan/EndTime']).timestamp())
                        meta_dict['end'] = end
                    except (TypeError, ValueError):
                        pass
            elif resource_type == 'Reserved Instances':
                if not payment_option and 'pricing/PurchaseOption' in e:
                    payment_option = e['pricing/PurchaseOption']
                    meta_dict['payment_option'] = payment_option
                if not offering_type and 'pricing/OfferingClass' in e:
                    offering_type = e['pricing/OfferingClass']
                    meta_dict['offering_type'] = offering_type
                if not purchase_term and 'pricing/LeaseContractLength' in e:
                    purchase_term = e['pricing/LeaseContractLength']
                    meta_dict['purchase_term'] = purchase_term
                if not start and 'reservation/StartTime' in e:
                    try:
                        start = int(self._datetime_from_value(
                            e['reservation/StartTime']).timestamp())
                        meta_dict['start'] = start
                    except (TypeError, ValueError):
                        pass
                if not end and 'reservation/EndTime' in e:
                    try:
                        end = int(self._datetime_from_value(
                            e['reservation/EndTime']).timestamp())
                        meta_dict['end'] = end
                    except (TypeError, ValueError):
                        pass
                if not instance_type and 'lineItem/UsageType' in e:
                    instance_type = e['lineItem/UsageType'].split(
                        ':')[-1]
                    meta_dict['instance_type'] = instance_type
                if not platform and 'lineItem/LineItemDescription' in e:
                    platform = e['lineItem/LineItemDescription']
                    for ri_platform in RI_PLATFORMS:
                        if ri_platform in platform:
                            meta_dict['platform'] = ri_platform
                            break
                if not zone and 'reservation/AvailabilityZone' in e:
                    zone = e['reservation/AvailabilityZone']
                    meta_dict['instance_type'] = zone
        for product_family_value in self.main_resources_product_family_map.get(
                resource_type, []):
            family_region = family_region_map.get(product_family_value, None)
            if family_region:
                region = family_region
                break
        if region is None:
            region = last_region
        if last_seen < first_seen:
            last_seen = first_seen

        if resource_type in ['Reserved Instances', 'Savings Plan']:
            name = None
            tags = {}

        info = {
            'name': name,
            'type': resource_type,
            'region': region,
            'service_name': service_name,
            'tags': tags,
            'first_seen': int(first_seen.timestamp()),
            'last_seen': int(last_seen.timestamp()),
            **fake_cad_extras,
            **meta_dict
        }
        LOG.debug('Detected resource info: %s', info)
        return info

    @staticmethod
    def get_resource_type(raw_expense, resource_type):
        item_type = raw_expense.get('lineItem/LineItemType')
        usage_type = raw_expense.get('lineItem/UsageType')
        operation = raw_expense.get('lineItem/Operation')
        tax_type = raw_expense.get('lineItem/TaxType')
        product = raw_expense.get('lineItem/ProductCode')
        product_family = raw_expense.get('product/productFamily')
        product_name = raw_expense.get('product/ProductName')
        resource_id = raw_expense.get('lineItem/ResourceId')
        ri_id = raw_expense.get('reservation/ReservationARN')
        sp_id = raw_expense.get('savingsPlan/SavingsPlanARN')
        ip_address_type = 'IP Address'
        nat_gateway_type = 'NAT Gateway'
        instance_type = 'Instance'
        snapshot_type = 'Snapshot'
        volume_type = 'Volume'
        bucket_type = 'Bucket'
        sp_type = 'Savings Plan'
        ri_type = 'Reserved Instances'
        lb_type = 'Load Balancer'

        def extract_type_by_product_type(res_type):
            return product_family and res_type in product_family

        def extract_data_transfer():
            return (resource_id and 'natgateway' not in resource_id and
                    product_family and 'Data Transfer' in product_family)

        resource_type_map = OrderedDict()
        if tax_type:
            resource_type_map[tax_type] = item_type == 'Tax'
        resource_type_map.update({
            nat_gateway_type: extract_type_by_product_type(nat_gateway_type),
            instance_type: (usage_type and operation and
                            'SavingsPlan' not in item_type and
                            (extract_type_by_product_type(instance_type) or
                             extract_data_transfer()) and
                            ('BoxUsage' in usage_type or instance_type in
                             operation)),
            snapshot_type: usage_type and operation and (
                    snapshot_type in usage_type or snapshot_type in operation),
            volume_type: usage_type and volume_type in usage_type,
            'Bucket': product and 'AmazonS3' in product and (
                    bool(resource_id) and bucket_type in operation),
            ip_address_type: (extract_type_by_product_type(ip_address_type) or
                              usage_type and 'PublicIP' in usage_type),
            sp_type: bool(sp_id) and 'SavingsPlan' in item_type,
            ri_type: bool(ri_id),
            lb_type: (extract_type_by_product_type(lb_type) or
                      product_name == 'Elastic Load Balancing'),
            'Other': (tax_type or resource_type or product_family or
                      usage_type or item_type)
        })
        resource_type_keys = [k for k, v in resource_type_map.items()
                              if v is True]
        return (resource_type_keys[0] if resource_type_keys else
                resource_type_map.get('Other'))

    def get_resource_info_map(self, chunk):
        regular_res_info_map = {}
        sp_covered_chunk = defaultdict(list)
        for r_id, expenses in chunk.items():
            # collect regular resources infos
            regular_res_info_map[r_id] = self.get_resource_info_from_expenses(
                expenses)

            # collect instances covered by SP info map to create them due to
            # these instances may not have own raw expenses as their expenses
            # are related to SP resource
            for exp in expenses:
                if exp['lineItem/LineItemType'] == 'SavingsPlanCoveredUsage':
                    resource_id = self.short_resource_id(
                        exp['lineItem/ResourceId'])
                    sp_covered_chunk[resource_id].append(exp)

        # use predefined resource type to avoid detecting SP fields
        info_map = {
            r_id: self.get_resource_info_from_expenses(
                expenses, resource_type='Instance')
            for r_id, expenses in sp_covered_chunk.items()
        }
        info_map.update(regular_res_info_map)
        return info_map

    def _update_imported_raw_interval(self, expense):
        if (expense['start_date'] < self.min_date_import_threshold and
                expense.get('lineItem/LineItemType') == 'RIFee'):
            # diworker update RIFee expenses on every import no matter on
            # start_date, so can't use its start_date for clearing rudiments
            return
        super()._update_imported_raw_interval(expense)

    def clean_expenses_for_resource(self, resource_id, expenses):
        clean_expenses = {}
        for e in expenses:
            start_date = self._datetime_from_expense(
                e, 'start_date')
            end_date = self._datetime_from_expense(
                e, 'lineItem/UsageEndDate')
            # end date may point to the 00:00 on the next day,
            # so to avoid confusion removing one second
            end_date -= timedelta(seconds=1)
            days = (end_date - start_date).days + 1
            for d in range(days):
                date = start_date + timedelta(days=d)
                day = date.replace(hour=0, minute=0, second=0, microsecond=0)
                if day in clean_expenses:
                    clean_expenses[day]['cost'] += float(
                        e['lineItem/BlendedCost']) / days
                else:
                    clean_expenses[day] = {
                        'date': day,
                        'cost': float(e['lineItem/BlendedCost']) / days,
                        'resource_id': resource_id,
                        'cloud_account_id': e['cloud_account_id']
                    }
        return clean_expenses

    @staticmethod
    def _get_group_by_day_pipeline():
        unique_keys = AWSReportImporter.get_unique_field_list(
            include_date=False)
        day_group_id_pipeline = {k: '$%s' % k for k in unique_keys}
        day_group_id_pipeline.update({
            'start_date': {
                'month': {'$month': "$start_date"},
                'day': {'$dayOfMonth': "$start_date"},
                'year': {'$year': "$start_date"},
            },
            'end_date': {
                'month': {'$month': "$end_date"},
                'day': {'$dayOfMonth': "$end_date"},
                'year': {'$year': "$end_date"},
            },
            'resource_id': '$resource_id',
        })
        day_group_pipeline = {
            '_id': day_group_id_pipeline,
            "root": {"$first": "$$ROOT"},
            'resource_id': {'$first': "$resource_id"},
            'lineItem/UsageStartDate': {"$min": "$start_date"},
            'lineItem/UsageEndDate': {"$max": "$end_date"},
            'lineItem/BlendedCost': {'$sum': '$cost'}
        }
        return day_group_pipeline

    def get_resource_ids(self, cloud_account_id, billing_period):
        filters = {
            'cloud_account_id': cloud_account_id,
            'resource_id': {'$exists': True, '$ne': None},
            'start_date': {'$gte': self.min_date_import_threshold}
        }
        if billing_period:
            filters['bill/BillingPeriodStartDate'] = billing_period
        resource_ids = self.mongo_raw.aggregate([
            {'$match': filters},
            {'$group': {'_id': '$resource_id'}},
        ], allowDiskUse=True)
        return [x['_id'] for x in resource_ids]

    def get_raw_expenses_by_filters(self, filters):
        return self.mongo_raw.aggregate([
                {'$match': {
                    '$and': filters,
                }},
                {'$group': self._get_group_by_day_pipeline()},
                {'$replaceRoot': {'newRoot': {
                    '$mergeObjects': ["$root", "$$ROOT"]}
                }},
                {'$project': {"root": 0}}
            ], allowDiskUse=True)

    def _get_billing_period_filters(self, billing_period):
        return {
            'bill/BillingPeriodStartDate': billing_period,
            'start_date': {'$gte': self.min_date_import_threshold}
        }

    @staticmethod
    def set_raw_chunk(expenses):
        chunk = defaultdict(list)
        for ex in expenses:
            resource_id = ex['resource_id']
            chunk[resource_id].append(ex)
        return chunk

    def compose_resource_id(self, expense):
        item_type = expense['lineItem/LineItemType']
        sp_id = expense.get('savingsPlan/SavingsPlanARN')
        ri_id = expense.get('reservation/ReservationARN')
        if item_type in IGNORE_EXPENSE_TYPES:
            return
        elif 'SavingsPlan' in item_type and sp_id:
            return self.short_resource_id(sp_id)
        elif ri_id:
            return self.short_resource_id(ri_id)
        parts = self.ITEM_TYPE_ID_FIELDS.get(item_type)
        if parts:
            resource_id = ' '.join([expense.get(k)
                                    for k in parts if k in expense])
            return resource_id
        else:
            return expense.get('lineItem/LineItemDescription')

    def _get_cloud_extras(self, info):
        res = defaultdict(dict)
        for k in ['os', 'preinstalled', 'payment_option', 'offering_type',
                  'purchase_term', 'applied_region', 'start', 'end', 'platform',
                  'instance_type', 'zone']:
            val = info.get(k)
            if val:
                res['meta'][k] = val
        return res

    def update_cloud_account_config(self):
        config = self.cloud_acc.get('config')
        if not config.get('cur_version'):
            cur_version = self.cloud_adapter.config.get('cur_version')
            if cur_version:
                config.pop('region_name', None)
                config.update({'cur_version': cur_version})
                self.rest_cl.cloud_account_update(
                    self.cloud_acc_id, {'config': config})

    def create_traffic_processing_tasks(self):
        self._create_traffic_processing_tasks()

    def create_risp_processing_tasks(self):
        self._create_risp_processing_tasks()

    def cleanup(self):
        """Override cleanup to log cache statistics and perform cleanup."""
        # Log cache statistics
        if self._report_cache:
            cache_stats = self.report_cache.get_cache_stats()
            LOG.info(f"AWS Report Cache Statistics: {cache_stats}")
            LOG.info(f"Uncompressed file cache - Hits: {self.cache_stats['hits']}, "
                    f"Misses: {self.cache_stats['misses']}")

            # Perform cache cleanup
            try:
                cleanup_stats = self.report_cache.cleanup_old_files()
                if sum(cleanup_stats.values()) > 0:
                    LOG.info(f"Cache cleanup: {cleanup_stats}")
            except Exception as e:
                LOG.warning(f"Error during cache cleanup: {e}")

        # Call parent cleanup
        super().cleanup()

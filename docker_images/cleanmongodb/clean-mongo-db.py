import json
import os
import time
import etcd
import logging
from bson.objectid import ObjectId
from optscale_client.config_client.client import Client as ConfigClient
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from retrying import retry

ARCHIVE_ENABLED = False
ARCHIVE_PATH = '/src/archive'
FILE_MAX_ROWS = 10000
CHUNK_SIZE = 500
ROWS_LIMIT = 10000
DEFAULT_STOP_MAX_ATTEMPT_NUMBER = 5
DEFAULT_RETRY_ARGS = dict(stop_max_attempt_number=10, wait_fixed=1000)
RAW_EXPENSES_TTL_DAYS = 180  # 6 months; 0 disables time-based cleanup
# Maximum wall-clock seconds the entire job is allowed to run.
# When the deadline is reached, the current batch completes and no new
# batches are started. Work done so far is committed. The job exits cleanly
# and resumes from where it left off on the next scheduled run.
MAX_RUNTIME_SECS = 6 * 3600  # 6 hours
# Compact rewrites data files and can itself take a long time on large
# collections. Only attempt it if this many seconds remain on the deadline.
COMPACT_MIN_SECS_REMAINING = 1800  # 30 minutes
# Documents deleted per delete+compact cycle when compact_after_purge is on.
# Smaller values produce more frequent compacts (shorter individual lock
# windows, lower peak fragmentation) at the cost of more total compact
# wall-time. The full purge runs up to ROWS_LIMIT // COMPACT_CYCLE_SIZE
# cycles per job.
COMPACT_CYCLE_SIZE = 50000

LOG = logging.getLogger(__name__)


def _retry(exc):
    if isinstance(exc, etcd.EtcdKeyNotFound):
        return True
    return False


class CleanMongoDB(object):
    def __init__(self):
        super().__init__()
        self._config_client = None
        self._mongo_client = None
        self._archive_enable = ARCHIVE_ENABLED
        self._file_max_rows = FILE_MAX_ROWS
        self._chunk_size = CHUNK_SIZE
        self._compact_cycle_size = COMPACT_CYCLE_SIZE
        self._limits = {
            # linked to cloud_account_id
            self.mongo_client.restapi.raw_expenses: ROWS_LIMIT,
            self.mongo_client.restapi.resources: ROWS_LIMIT,
            # linked to organization_id
            self.mongo_client.restapi.archived_recommendations: ROWS_LIMIT,
            self.mongo_client.restapi.checklists: ROWS_LIMIT,
            self.mongo_client.restapi.webhook_observer: ROWS_LIMIT,
            self.mongo_client.restapi.webhook_logs: ROWS_LIMIT,
            # linked to organization_id
            self.mongo_client.keeper.event: ROWS_LIMIT,
        }

    @property
    def config_client(self):
        if not self._config_client:
            etcd_host = os.environ.get('HX_ETCD_HOST')
            etcd_port = int(os.environ.get('HX_ETCD_PORT'))
            self._config_client = ConfigClient(host=etcd_host, port=etcd_port)
        return self._config_client

    @retry(**DEFAULT_RETRY_ARGS, retry_on_exception=_retry)
    def get_mongo_params(self, config_client):
        return config_client.mongo_params()

    @property
    def mongo_client(self):
        if not self._mongo_client:
            mongo_params = self.get_mongo_params(self.config_client)
            # Configure MongoDB client with explicit timeouts and connection pool settings
            # to prevent connection refused errors during cleanup operations
            self._mongo_client = MongoClient(
                mongo_params[0],
                serverSelectionTimeoutMS=30000,  # 30 seconds to select a server
                connectTimeoutMS=20000,          # 20 seconds to establish connection
                socketTimeoutMS=60000,           # 60 seconds for socket operations
                maxPoolSize=50,                  # Maximum connections in pool
                minPoolSize=10,                  # Minimum connections to maintain
                maxIdleTimeMS=300000,            # 5 minutes before idle connections are closed
                retryWrites=True,                # Automatically retry write operations
                retryReads=True                  # Automatically retry read operations
            )
        return self._mongo_client

    def get_settings(self):
        try:
            result = self.config_client.read_branch('/cleanmongodb')
        except Exception as exc:
            LOG.error(f'Error getting settings for cleaner: {exc}, '
                      f'will use default values')
            result = {}
        return result

    def get_session(self):
        engine = create_engine(
            'mysql+mysqlconnector://%s:%s@%s/%s?charset=utf8mb4' %
            self.config_client.rest_db_params(),
            pool_size=200,
            max_overflow=25,
            pool_pre_ping=True,
        )
        return Session(bind=engine)

    @property
    def limits(self):
        return self._limits

    @limits.setter
    def limits(self, value):
        self._limits = value

    @property
    def chunk_size(self):
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, value):
        self._chunk_size = value

    @property
    def compact_cycle_size(self):
        return self._compact_cycle_size

    @compact_cycle_size.setter
    def compact_cycle_size(self, value):
        self._compact_cycle_size = value

    @property
    def archive_enable(self):
        return self._archive_enable

    @archive_enable.setter
    def archive_enable(self, value):
        self._archive_enable = value

    @property
    def file_max_rows(self):
        return self._file_max_rows

    @file_max_rows.setter
    def file_max_rows(self, value):
        self._file_max_rows = value

    def get_deleted_organization_info(self):
        result = None
        session = self.get_session()
        stmt = """
            SELECT organization.id
            FROM organization
            LEFT JOIN profiling_token as p_token
            ON organization.id = p_token.organization_id
            WHERE organization.deleted_at != 0 AND
                organization.cleaned_at = 0
            LIMIT 1
        """
        try:
            result = next(session.execute(stmt))
        except StopIteration:
            pass
        finally:
            session.close()
        return result

    def delete_in_chunks(self, collection, filter_name, filter_value,
                         archive=False, archive_cloud_account_id=None):
        rows_limit = self.limits.get(collection, 0)
        if not rows_limit:
            return 0
        if isinstance(filter_value, list):
            filter_value = {'$in': filter_value}
        row_ids = list(collection.find({filter_name: filter_value}, ['_id']
                                       ).limit(rows_limit))
        for j in range(0, len(row_ids), self.chunk_size):
            chunk = [row['_id'] for row in row_ids[j: j + self.chunk_size]]
            if archive:
                LOG.info('Archiving raw expenses')
                last_file_name = self.get_archive_file(
                    archive_cloud_account_id)
                file_length = self.get_file_length(last_file_name)
                split_data = self.split_chunk_by_files(
                    chunk, self.file_max_rows - file_length, last_file_name,
                    archive_cloud_account_id, self.file_max_rows)
                for file, raw_expenses in split_data.items():
                    path = os.path.join(ARCHIVE_PATH, file)
                    exp_count = len(raw_expenses)
                    full_expenses_rows = collection.find(
                        {'_id': {'$in': raw_expenses}})
                    with open(path, 'a+') as f:
                        if raw_expenses:
                            LOG.info(
                                f'Saving {exp_count} expenses to file {path}')
                            for row in full_expenses_rows:
                                f.write(self._row_to_json(row) + '\n')
            collection.delete_many({'_id': {'$in': chunk}})
        remainder_rows = rows_limit - len(row_ids)
        return remainder_rows

    def get_deleted_cloud_account(self):
        result = (None, None)
        session = self.get_session()
        stmt = """SELECT cloudaccount.id, organization.is_demo
                  FROM cloudaccount
                  JOIN organization
                  ON organization.id = cloudaccount.organization_id
                  WHERE (organization.deleted_at != 0
                  OR cloudaccount.deleted_at != 0)
                  AND cloudaccount.cleaned_at = 0
                  LIMIT 1"""
        try:
            result = next(session.execute(stmt))
        except StopIteration:
            pass
        finally:
            session.close()
        return result

    def update_cleaned_at(self, cloud_account_id=None, organization_id=None):
        session = self.get_session()
        now = int(datetime.now(tz=timezone.utc).timestamp())
        if cloud_account_id:
            LOG.info(
                f'Updating cleaned_at for cloud account {cloud_account_id}')
            stmt = f"""UPDATE cloudaccount
                       SET cleaned_at={now} WHERE id='{cloud_account_id}'"""
        elif organization_id:
            LOG.info(
                f'Updating cleaned_at for organization {organization_id}')
            stmt = f"""UPDATE organization
                       SET cleaned_at={now} WHERE id='{organization_id}'"""
        else:
            return
        try:
            session.execute(stmt)
            session.commit()
        finally:
            session.close()

    def get_archive_file(self, cloud_account_id):
        last_file_name = f'{cloud_account_id}_1.json'
        os.makedirs(ARCHIVE_PATH, exist_ok=True)
        files = os.listdir(ARCHIVE_PATH)
        cloud_acc_files = sorted(
            [x for x in files if cloud_account_id in x],
            key=lambda x: self.get_file_number(x), reverse=True)
        if cloud_acc_files:
            last_file_name = cloud_acc_files[0]
        return last_file_name

    @staticmethod
    def get_file_length(filename):
        path = os.path.join(ARCHIVE_PATH, filename)
        if not os.path.isfile(path):
            result = 0
        else:
            with open(path, 'r') as f:
                result = len(f.readlines())
        return result

    @staticmethod
    def get_file_number(filename):
        return int(filename.split('_')[1].split('.json')[0])

    @staticmethod
    def _row_to_json(row):
        # wrap not json data types to use by mongoimport
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = {'$date': v.strftime('%Y-%m-%dT%H:%M:%SZ')}
            elif isinstance(v, ObjectId):
                row[k] = {'$oid': str(v)}
        return json.dumps(row)

    def split_chunk_by_files(self, chunk, available_rows_count, filename,
                             cloud_account_id, file_max_rows):
        result = {}
        if available_rows_count < 0:
            available_rows_count = 0
        result[filename] = chunk[:available_rows_count]
        new_filename = filename
        for i in range(available_rows_count, len(chunk), file_max_rows):
            count = self.get_file_number(new_filename) + 1
            new_filename = f'{cloud_account_id}_{count}.json'
            result[new_filename] = chunk[i:i+file_max_rows]
        return result

    def _delete_by_organization(self, org_id):
        restapi_collections = [
            self.mongo_client.restapi.archived_recommendations,
            self.mongo_client.restapi.checklists,
            # delete clusters resources
            self.mongo_client.restapi.resources,
            self.mongo_client.restapi.webhook_observer,
            self.mongo_client.restapi.webhook_logs
        ]
        keeper_collections = [
            self.mongo_client.keeper.event
        ]
        LOG.info('Start processing objects for organization %s', org_id)
        for collection in keeper_collections:
            self.limits[collection] = self.delete_in_chunks(
                collection, 'organization_id', org_id)
        for collection in restapi_collections:
            self.limits[collection] = self.delete_in_chunks(
                collection, 'organization_id', org_id)
        if all(limit > 0 for collection, limit in self.limits.items()
               if collection in restapi_collections + keeper_collections):
            self.update_cleaned_at(organization_id=org_id)

    def organization_limits(self):
        collections = [
            self.mongo_client.restapi.archived_recommendations,
            self.mongo_client.restapi.checklists,
            self.mongo_client.restapi.resources,
            self.mongo_client.restapi.webhook_observer,
            self.mongo_client.restapi.webhook_logs,
            self.mongo_client.keeper.event
        ]
        return [self.limits[x] for x in collections]

    def delete_by_organization(self, deadline=None):
        info = self.get_deleted_organization_info()
        if not info:
            return
        while info and all(limit > 0 for limit in self.organization_limits()):
            if deadline is not None and time.monotonic() >= deadline:
                LOG.warning(
                    'delete_by_organization: deadline reached, stopping early')
                break
            self._delete_by_organization(info[0])
            info = self.get_deleted_organization_info()
        LOG.info('Organizations objects processing is completed')

    def _delete_by_cloud_account(self, cloud_account_id, is_demo):
        restapi_collections = [self.mongo_client.restapi.raw_expenses,
                               self.mongo_client.restapi.resources]
        LOG.info(f'Started processing for cloud account {cloud_account_id}')
        for collection in restapi_collections:
            archive = False
            if (collection == self.mongo_client.restapi.raw_expenses
                    and self.archive_enable and not is_demo):
                archive = True
            self.limits[collection] = self.delete_in_chunks(
                    collection, 'cloud_account_id', cloud_account_id,
                    archive=archive, archive_cloud_account_id=cloud_account_id)
        if all(limit > 0 for collection, limit in self.limits.items()
               if collection in restapi_collections):
            self.update_cleaned_at(cloud_account_id=cloud_account_id)

    def cloud_account_limits(self):
        collections = [self.mongo_client.restapi.resources,
                       self.mongo_client.restapi.raw_expenses]
        return [self.limits[x] for x in collections]

    def delete_by_cloud_account(self, deadline=None):
        cloud_account_id, is_demo = self.get_deleted_cloud_account()
        while cloud_account_id and all(
                limit > 0 for limit in self.cloud_account_limits()):
            if deadline is not None and time.monotonic() >= deadline:
                LOG.warning(
                    'delete_by_cloud_account: deadline reached, stopping early')
                break
            self._delete_by_cloud_account(cloud_account_id, is_demo)
            cleaned_cloud_account_id = cloud_account_id
            cloud_account_id, is_demo = self.get_deleted_cloud_account()
            if cleaned_cloud_account_id == cloud_account_id:
                # last cloud account that can't be cleaned up in current
                # iteration, should be cleaned in the next run
                break
        LOG.info('Cloud accounts processing is completed')

    def _compact_raw_expenses(self):
        """
        Run db.compact('raw_expenses') and log size delta.  Returns True on
        success, False on failure (caller decides whether to keep cycling).
        Errors are caught — deletions in this cycle stay committed.
        """
        try:
            # MongoDB refuses compact on a replica-set primary without
            # force:true (single-replica deployments are always primary).
            # Detect role at runtime so the same code works on both
            # single-node and multi-node replica sets.
            is_primary = self.mongo_client.admin.command(
                'isMaster').get('ismaster', False)
            compact_cmd = {'compact': 'raw_expenses'}
            if is_primary:
                compact_cmd['force'] = True

            stats_before = self.mongo_client.restapi.command(
                'collStats', 'raw_expenses')
            size_before_mb = stats_before.get('storageSize', 0) / (1024 * 1024)

            result = self.mongo_client.restapi.command(compact_cmd)

            stats_after = self.mongo_client.restapi.command(
                'collStats', 'raw_expenses')
            size_after_mb = stats_after.get('storageSize', 0) / (1024 * 1024)

            LOG.info(
                'compact complete: storageSize %.1f MB → %.1f MB '
                '(freed %.1f MB). result: %s',
                size_before_mb, size_after_mb,
                size_before_mb - size_after_mb, result)
            return True
        except Exception as exc:
            LOG.warning(
                'compact failed (non-fatal, deletions were committed): %s',
                exc)
            return False

    def purge_old_raw_expenses(self, ttl_days, run_compact=False, deadline=None):
        """
        Delete raw_expenses documents whose start_date is older than
        ttl_days across all cloud accounts.

        Uses the same find-then-delete-by-id pattern as delete_in_chunks to
        avoid long-running deletes that can cause MongoDB lock contention.
        The number of documents removed per job run is bounded by the
        configured rows_limit so the job stays predictably short.

        run_compact: when True, runs db.compact('raw_expenses') on the primary
        in cycles — every COMPACT_CYCLE_SIZE deletes triggers a compact so
        space is reclaimed incrementally and a deadline interruption never
        leaves the collection bloated with already-deleted docs.  WiredTiger
        marks deleted pages as reusable but does NOT return bytes to the OS
        until compact rewrites the data files.  compact briefly impacts
        reads/writes on the collection, so it is off by default; enable via
        compact_after_purge: true in cleanmongodb config.
        """
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=ttl_days)
        collection = self.mongo_client.restapi.raw_expenses
        rows_limit = self.limits.get(collection, ROWS_LIMIT)
        cycle_size = self.compact_cycle_size

        delete_filter = {
            'start_date': {'$exists': True, '$lt': cutoff}
        }

        # Estimate pending work — counts can be slow on large collections so
        # fall back gracefully if the count query exceeds its time limit.
        try:
            pending = collection.count_documents(
                delete_filter, maxTimeMS=20000)
            LOG.info(
                'purge_old_raw_expenses: ~%d documents older than %d days '
                '(cutoff %s), will remove up to %d this run, '
                'cycle=%d (compact=%s)',
                pending, ttl_days, cutoff.date(), rows_limit,
                cycle_size, run_compact)
        except Exception as exc:
            LOG.warning(
                'Could not estimate old raw_expenses count: %s', exc)
            LOG.info(
                'purge_old_raw_expenses: ~? documents older than %d days '
                '(cutoff %s), will remove up to %d this run, '
                'cycle=%d (compact=%s)',
                ttl_days, cutoff.date(), rows_limit, cycle_size, run_compact)

        total_deleted = 0
        cycle_num = 0
        exhausted = False
        while total_deleted < rows_limit and not exhausted:
            if deadline is not None and time.monotonic() >= deadline:
                LOG.warning(
                    'purge_old_raw_expenses: deadline reached after deleting '
                    '%d documents in %d cycle(s) — stopping, work is committed',
                    total_deleted, cycle_num)
                break

            cycle_num += 1
            cycle_target = min(cycle_size, rows_limit - total_deleted)
            cycle_deleted = 0
            while cycle_deleted < cycle_target:
                if deadline is not None and time.monotonic() >= deadline:
                    LOG.warning(
                        'purge_old_raw_expenses: deadline reached mid-cycle %d '
                        'after deleting %d documents — stopping',
                        cycle_num, total_deleted + cycle_deleted)
                    break
                batch_size = min(self.chunk_size, cycle_target - cycle_deleted)
                ids = [
                    doc['_id']
                    for doc in collection.find(
                        delete_filter, {'_id': 1}
                    ).limit(batch_size)
                ]
                if not ids:
                    exhausted = True
                    break
                collection.delete_many({'_id': {'$in': ids}})
                cycle_deleted += len(ids)
                if len(ids) < batch_size:
                    exhausted = True
                    break

            total_deleted += cycle_deleted
            LOG.info(
                'purge_old_raw_expenses: cycle %d deleted %d docs '
                '(total %d / %d)',
                cycle_num, cycle_deleted, total_deleted, rows_limit)

            if cycle_deleted == 0:
                break
            if not run_compact:
                continue
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining < COMPACT_MIN_SECS_REMAINING:
                    LOG.warning(
                        'compact skipped for cycle %d — only %.0f seconds '
                        'remain on deadline (need at least %d). '
                        'Will resume next scheduled job.',
                        cycle_num, remaining, COMPACT_MIN_SECS_REMAINING)
                    break
            LOG.info(
                'cycle %d: running compact on raw_expenses after deleting '
                '%d documents this cycle (%d total). Reads/writes briefly '
                'impacted.', cycle_num, cycle_deleted, total_deleted)
            self._compact_raw_expenses()

        LOG.info(
            'purge_old_raw_expenses: finished — deleted %d documents older '
            'than %d days across %d cycle(s)',
            total_deleted, ttl_days, cycle_num)

    def clean_mongo(self):
        settings = self.get_settings()
        self.archive_enable = bool(
            settings.get('archive_enable', False)) or ARCHIVE_ENABLED
        self.file_max_rows = int(settings.get(
            'file_max_rows', 0)) or FILE_MAX_ROWS
        self.chunk_size = int(settings.get('chunk_size') or CHUNK_SIZE)
        self.compact_cycle_size = int(
            settings.get('compact_cycle_size') or COMPACT_CYCLE_SIZE)
        rows_limit = int(settings.get('rows_limit') or ROWS_LIMIT)
        for collection in self.limits:
            self.limits[collection] = rows_limit

        max_runtime_secs = int(
            settings.get('max_runtime_secs', MAX_RUNTIME_SECS))
        deadline = time.monotonic() + max_runtime_secs
        LOG.info(
            'clean_mongo: started, deadline in %d seconds (%d hours)',
            max_runtime_secs, max_runtime_secs // 3600)

        self.delete_by_cloud_account(deadline=deadline)
        self.delete_by_organization(deadline=deadline)
        ttl_days = int(
            settings.get('raw_expenses_ttl_days', RAW_EXPENSES_TTL_DAYS))
        if ttl_days > 0:
            compact_after_purge = bool(
                settings.get('compact_after_purge', False))
            self.purge_old_raw_expenses(
                ttl_days, run_compact=compact_after_purge, deadline=deadline)
        LOG.info('Processing completed')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cleanmongo = CleanMongoDB()
    cleanmongo.clean_mongo()

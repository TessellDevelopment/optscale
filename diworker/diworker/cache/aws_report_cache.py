#!/usr/bin/env python
import hashlib
import logging
import os
import fcntl
import time
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any

LOG = logging.getLogger(__name__)

# Default configuration
DEFAULT_CACHE_DIR = '/var/cache/optscale/aws_reports'
DEFAULT_CACHE_TTL_DAYS = 2
DEFAULT_LOCK_TIMEOUT = 300  # 5 minutes


class AWSReportCache:
    """
    Manages caching of AWS Cost and Usage Reports to avoid redundant downloads
    across multiple datasources sharing the same billing account.
    """
    
    def __init__(self, cache_dir: str = None, cache_ttl_days: int = None):
        """
        Initialize the AWS Report Cache.
        
        Args:
            cache_dir: Directory to store cached reports
            cache_ttl_days: Number of days to keep cached reports
        """
        self.cache_dir = cache_dir or os.environ.get(
            'AWS_REPORT_CACHE_DIR', DEFAULT_CACHE_DIR)
        self.cache_ttl_days = cache_ttl_days or int(os.environ.get(
            'AWS_REPORT_CACHE_TTL_DAYS', DEFAULT_CACHE_TTL_DAYS))
        self.compressed_dir = os.path.join(self.cache_dir, 'compressed')
        self.uncompressed_dir = os.path.join(self.cache_dir, 'uncompressed')
        self.locks_dir = os.path.join(self.cache_dir, 'locks')

        # Create cache directories
        os.makedirs(self.compressed_dir, exist_ok=True)
        os.makedirs(self.uncompressed_dir, exist_ok=True)
        os.makedirs(self.locks_dir, exist_ok=True)

        # Clean up stale locks on initialization (locks older than 1 hour)
        self._cleanup_stale_locks()

    def _cleanup_stale_locks(self):
        """
        Clean up stale lock files that are older than 1 hour.
        This prevents lock accumulation from crashed processes.
        """
        try:
            if not os.path.exists(self.locks_dir):
                return

            current_time = datetime.now().timestamp()
            stale_threshold = 3600  # 1 hour in seconds
            removed_count = 0

            for lock_file in os.listdir(self.locks_dir):
                if not lock_file.endswith('.lock'):
                    continue

                lock_path = os.path.join(self.locks_dir, lock_file)
                try:
                    # Check file age
                    file_age = current_time - os.path.getmtime(lock_path)

                    if file_age > stale_threshold:
                        os.remove(lock_path)
                        removed_count += 1
                        LOG.debug(f"Removed stale lock file: {lock_file} (age: {file_age/60:.1f} minutes)")
                except Exception as e:
                    LOG.warning(f"Error cleaning up lock file {lock_file}: {e}")

            if removed_count > 0:
                LOG.info(f"Cleaned up {removed_count} stale lock files")
        except Exception as e:
            LOG.warning(f"Error during lock cleanup: {e}")

    @staticmethod
    def _generate_cache_key(bucket_name: str, bucket_prefix: str,
                           report_name: str, s3_key: str) -> str:
        """
        Generate a unique cache key based on billing account configuration.
        
        Args:
            bucket_name: S3 bucket name
            bucket_prefix: S3 bucket prefix
            report_name: CUR report name
            s3_key: Full S3 object key
        
        Returns:
            Cache key (hash of the combination)
        """
        # Combine all identifiers
        identifier = f"{bucket_name}|{bucket_prefix}|{report_name}|{s3_key}"
        # Generate SHA256 hash
        hash_obj = hashlib.sha256(identifier.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def _get_compressed_path(self, cache_key: str, extension: str = '') -> str:
        """Get path for compressed file."""
        filename = cache_key + extension
        return os.path.join(self.compressed_dir, filename)
    
    def _get_uncompressed_path(self, cache_key: str, extension: str = '') -> str:
        """Get path for uncompressed file."""
        filename = cache_key + extension
        return os.path.join(self.uncompressed_dir, filename)
    
    def _get_lock_path(self, cache_key: str) -> str:
        """Get path for lock file."""
        return os.path.join(self.locks_dir, f"{cache_key}.lock")
    
    def _acquire_lock(self, lock_path: str, timeout: int = DEFAULT_LOCK_TIMEOUT) -> Optional[Any]:
        """
        Acquire a file lock to prevent concurrent downloads.
        
        Args:
            lock_path: Path to lock file
            timeout: Maximum time to wait for lock
        
        Returns:
            Lock file object or None if timeout
        """
        start_time = time.time()
        lock_file = open(lock_path, 'w')
        
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                LOG.debug(f"Acquired lock: {lock_path}")
                return lock_file
            except IOError:
                if time.time() - start_time > timeout:
                    LOG.warning(f"Lock timeout after {timeout}s: {lock_path}")
                    lock_file.close()
                    return None
                LOG.debug(f"Waiting for lock: {lock_path}")
                time.sleep(1)
    
    def _release_lock(self, lock_file: Any) -> None:
        """Release a file lock."""
        if lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
                LOG.debug("Released lock")
            except Exception as e:
                LOG.warning(f"Error releasing lock: {e}")
    
    def _is_file_fresh(self, file_path: str, s3_last_modified: datetime) -> bool:
        """
        Check if cached file is fresh compared to S3 version.
        
        Args:
            file_path: Path to cached file
            s3_last_modified: LastModified timestamp from S3
        
        Returns:
            True if cached file is up-to-date
        """
        if not os.path.exists(file_path):
            return False
        
        file_mtime = datetime.fromtimestamp(
            os.path.getmtime(file_path), tz=timezone.utc)
        
        # File is fresh if it's newer than or equal to S3 version
        return file_mtime >= s3_last_modified

    def get_cached_compressed_file(self, bucket_name: str, bucket_prefix: str,
                                   report_name: str, s3_key: str,
                                   s3_last_modified: datetime,
                                   file_extension: str = '') -> Optional[str]:
        """
        Get cached compressed file if it exists and is fresh.

        Args:
            bucket_name: S3 bucket name
            bucket_prefix: S3 bucket prefix
            report_name: CUR report name
            s3_key: Full S3 object key
            s3_last_modified: LastModified timestamp from S3
            file_extension: File extension (e.g., '.gz', '.zip')

        Returns:
            Path to cached file or None if not found/stale
        """
        cache_key = self._generate_cache_key(
            bucket_name, bucket_prefix, report_name, s3_key)
        cached_path = self._get_compressed_path(cache_key, file_extension)

        if self._is_file_fresh(cached_path, s3_last_modified):
            LOG.info(f"Cache HIT for compressed file: {s3_key}")
            return cached_path

        LOG.info(f"Cache MISS for compressed file: {s3_key}")
        return None

    def store_compressed_file(self, source_path: str, bucket_name: str,
                             bucket_prefix: str, report_name: str, s3_key: str,
                             file_extension: str = '') -> str:
        """
        Store a compressed file in the cache.

        Args:
            source_path: Path to the file to cache
            bucket_name: S3 bucket name
            bucket_prefix: S3 bucket prefix
            report_name: CUR report name
            s3_key: Full S3 object key
            file_extension: File extension (e.g., '.gz', '.zip')

        Returns:
            Path to cached file
        """
        cache_key = self._generate_cache_key(
            bucket_name, bucket_prefix, report_name, s3_key)
        cached_path = self._get_compressed_path(cache_key, file_extension)

        # Copy file to cache atomically
        temp_path = cached_path + '.tmp'
        try:
            shutil.copy2(source_path, temp_path)
            os.rename(temp_path, cached_path)
            LOG.info(f"Stored compressed file in cache: {cached_path}")
        except Exception as e:
            LOG.error(f"Error storing compressed file in cache: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

        return cached_path

    def get_or_download_compressed_file(self, download_func, target_path: str,
                                       bucket_name: str, bucket_prefix: str,
                                       report_name: str, s3_key: str,
                                       s3_last_modified: datetime,
                                       file_extension: str = '') -> str:
        """
        Get cached compressed file or download it with locking.
        Now supports downloading directly to cache (when target_path is in cache).

        Args:
            download_func: Function to call to download the file
            target_path: Target path for download (can be cache path or datasource path)
            bucket_name: S3 bucket name
            bucket_prefix: S3 bucket prefix
            report_name: CUR report name
            s3_key: Full S3 object key
            s3_last_modified: LastModified timestamp from S3
            file_extension: File extension (e.g., '.gz', '.zip')

        Returns:
            Path to the file (cached or downloaded)
        """
        # Check cache first
        cached_file = self.get_cached_compressed_file(
            bucket_name, bucket_prefix, report_name, s3_key,
            s3_last_modified, file_extension)

        if cached_file:
            # File already in cache
            LOG.info(f"Cache HIT for compressed file: {s3_key}")
            # If target is already the cache path, we're done
            if os.path.abspath(cached_file) == os.path.abspath(target_path):
                return target_path
            # Otherwise copy from cache to target location (shouldn't happen with new design)
            shutil.copy2(cached_file, target_path)
            return target_path

        # Need to download - acquire lock
        cache_key = self._generate_cache_key(
            bucket_name, bucket_prefix, report_name, s3_key)
        lock_path = self._get_lock_path(cache_key)
        lock_file = self._acquire_lock(lock_path)

        if not lock_file:
            # Lock timeout - proceed with direct download
            LOG.warning(f"Lock timeout, downloading directly: {s3_key}")
            download_func()
            return target_path

        try:
            # Double-check cache after acquiring lock
            cached_file = self.get_cached_compressed_file(
                bucket_name, bucket_prefix, report_name, s3_key,
                s3_last_modified, file_extension)

            if cached_file:
                # Another process downloaded it
                LOG.info(f"File downloaded by another process: {s3_key}")
                # If target is the cache path, we're done
                if os.path.abspath(cached_file) == os.path.abspath(target_path):
                    return target_path
                # Copy from cache to target (fallback for old code paths)
                shutil.copy2(cached_file, target_path)
            else:
                # Download the file directly to target (which should be cache)
                LOG.info(f"Downloading file: {s3_key}")
                download_func()

                # If target is not in cache, store it there
                cache_path = self._get_compressed_path(cache_key, file_extension)
                if os.path.abspath(target_path) != os.path.abspath(cache_path):
                    # Target was not cache, need to move it
                    shutil.copy2(target_path, cache_path)
                    LOG.info(f"Stored compressed file in cache: {cache_path}")
                else:
                    LOG.info(f"Stored compressed file in cache: {cache_path}")
        finally:
            self._release_lock(lock_file)

        return target_path

    def get_cached_uncompressed_file(self, compressed_cache_key: str,
                                    file_extension: str = '') -> Optional[str]:
        """
        Get cached uncompressed file if it exists.

        Args:
            compressed_cache_key: Cache key of the compressed file
            file_extension: File extension (e.g., '.csv', '.parquet')

        Returns:
            Path to cached uncompressed file or None
        """
        cached_path = self._get_uncompressed_path(
            compressed_cache_key, file_extension)

        if os.path.exists(cached_path):
            LOG.info(f"Cache HIT for uncompressed file: {compressed_cache_key}")
            return cached_path

        LOG.info(f"Cache MISS for uncompressed file: {compressed_cache_key}")
        return None

    def store_uncompressed_file(self, source_path: str, compressed_cache_key: str,
                               file_extension: str = '') -> str:
        """
        Store an uncompressed file in the cache with race condition protection.

        Args:
            source_path: Path to the uncompressed file
            compressed_cache_key: Cache key of the compressed source
            file_extension: File extension (e.g., '.csv', '.parquet')

        Returns:
            Path to cached uncompressed file
        """
        cached_path = self._get_uncompressed_path(
            compressed_cache_key, file_extension)

        # Check if file already exists (race condition - another process finished first)
        if os.path.exists(cached_path):
            LOG.info(f"Uncompressed file already exists in cache: {cached_path}")
            return cached_path

        # Copy file to cache atomically
        temp_path = cached_path + '.tmp.' + str(os.getpid())  # Include PID for uniqueness
        try:
            shutil.copy2(source_path, temp_path)

            # Atomic rename - this will fail if another process already created the file
            try:
                os.rename(temp_path, cached_path)
                LOG.info(f"Stored uncompressed file in cache: {cached_path}")
            except OSError as rename_error:
                # Check if file exists (another process won the race)
                if os.path.exists(cached_path):
                    LOG.info(f"Another process stored the file first: {cached_path}")
                    # Clean up our temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                else:
                    # Rename failed for another reason
                    raise rename_error

        except Exception as e:
            LOG.error(f"Error storing uncompressed file in cache: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            raise

        return cached_path

    def create_symlink(self, target_path: str, link_path: str) -> bool:
        """
        Create a symlink from link_path to target_path with race condition handling.

        Args:
            target_path: Path to the actual file
            link_path: Path where symlink should be created

        Returns:
            True if symlink created, False if failed
        """
        try:
            # Create parent directory if needed
            os.makedirs(os.path.dirname(link_path), exist_ok=True)

            # Check if symlink already exists and points to correct target
            if os.path.islink(link_path):
                existing_target = os.readlink(link_path)
                if existing_target == target_path:
                    LOG.debug(f"Symlink already exists correctly: {link_path} -> {target_path}")
                    return True
                else:
                    # Points to wrong target, remove it
                    LOG.warning(f"Removing incorrect symlink: {link_path} -> {existing_target}")
                    os.remove(link_path)
            elif os.path.exists(link_path):
                # Regular file exists, remove it
                LOG.warning(f"Removing existing file to create symlink: {link_path}")
                os.remove(link_path)

            # Create symlink atomically
            temp_link = link_path + '.tmp.' + str(os.getpid())
            try:
                os.symlink(target_path, temp_link)
                os.rename(temp_link, link_path)
                LOG.info(f"Created symlink: {link_path} -> {target_path}")
                return True
            except FileExistsError:
                # Race condition - another process created it
                if os.path.exists(temp_link):
                    os.remove(temp_link)
                if os.path.islink(link_path):
                    LOG.info(f"Symlink created by another process: {link_path}")
                    return True
                raise
        except Exception as e:
            LOG.error(f"Error creating symlink: {e}")
            return False

    def cleanup_old_files(self) -> Dict[str, int]:
        """
        Remove cached files older than cache_ttl_days.

        Returns:
            Dictionary with counts of removed files
        """
        cutoff_time = time.time() - (self.cache_ttl_days * 86400)
        stats = {
            'compressed_removed': 0,
            'uncompressed_removed': 0,
            'locks_removed': 0
        }

        # Clean compressed files
        for filename in os.listdir(self.compressed_dir):
            filepath = os.path.join(self.compressed_dir, filename)
            try:
                if os.path.getmtime(filepath) < cutoff_time:
                    os.remove(filepath)
                    stats['compressed_removed'] += 1
                    LOG.debug(f"Removed old compressed file: {filepath}")
            except Exception as e:
                LOG.warning(f"Error removing {filepath}: {e}")

        # Clean uncompressed files
        for filename in os.listdir(self.uncompressed_dir):
            filepath = os.path.join(self.uncompressed_dir, filename)
            try:
                if os.path.getmtime(filepath) < cutoff_time:
                    os.remove(filepath)
                    stats['uncompressed_removed'] += 1
                    LOG.debug(f"Removed old uncompressed file: {filepath}")
            except Exception as e:
                LOG.warning(f"Error removing {filepath}: {e}")

        # Clean stale lock files (older than 1 hour)
        lock_cutoff_time = time.time() - 3600
        for filename in os.listdir(self.locks_dir):
            filepath = os.path.join(self.locks_dir, filename)
            try:
                if os.path.getmtime(filepath) < lock_cutoff_time:
                    os.remove(filepath)
                    stats['locks_removed'] += 1
                    LOG.debug(f"Removed old lock file: {filepath}")
            except Exception as e:
                LOG.warning(f"Error removing {filepath}: {e}")

        if sum(stats.values()) > 0:
            LOG.info(f"Cache cleanup completed: {stats}")

        return stats

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache.

        Returns:
            Dictionary with cache statistics
        """
        stats = {
            'cache_dir': self.cache_dir,
            'cache_ttl_days': self.cache_ttl_days,
            'compressed_files': 0,
            'uncompressed_files': 0,
            'total_size_mb': 0
        }

        # Count compressed files
        if os.path.exists(self.compressed_dir):
            stats['compressed_files'] = len(os.listdir(self.compressed_dir))
            for filename in os.listdir(self.compressed_dir):
                filepath = os.path.join(self.compressed_dir, filename)
                stats['total_size_mb'] += os.path.getsize(filepath) / (1024 * 1024)

        # Count uncompressed files
        if os.path.exists(self.uncompressed_dir):
            stats['uncompressed_files'] = len(os.listdir(self.uncompressed_dir))
            for filename in os.listdir(self.uncompressed_dir):
                filepath = os.path.join(self.uncompressed_dir, filename)
                stats['total_size_mb'] += os.path.getsize(filepath) / (1024 * 1024)

        stats['total_size_mb'] = round(stats['total_size_mb'], 2)

        return stats

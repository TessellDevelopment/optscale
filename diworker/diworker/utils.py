import logging
import threading
import time
from functools import wraps
from pymongo.errors import BulkWriteError, ConnectionFailure
from retrying import retry

LOG = logging.getLogger(__name__)

# ConnectionFailure is the base class for all connection-related errors,
# including AutoReconnect and its subclass NetworkTimeout, which are raised
# on socket timeouts:
#   "mongo-0…:27017: timed out (socketTimeoutMS: 600000.0ms, …)"
# BulkWriteError covers transient bulk-write failures (e.g. write-concern
# timeouts).  Both are safe to retry because all write operations use upsert,
# so re-running them produces the same result (idempotent).
_RETRYABLE_MONGO_ERRORS = (BulkWriteError, ConnectionFailure)

# Thread-local context used to attribute retry warnings to the specific
# MongoDB operation that triggered them. Each worker thread gets its own
# copy, so concurrent imports do not cross-pollute the attribution.
_op_context = threading.local()


def _current_op_name():
    return getattr(_op_context, 'name', None) or 'mongo_operation'


def _retry_on_mongo_error(exc):
    if isinstance(exc, _RETRYABLE_MONGO_ERRORS):
        LOG.warning(
            'Retryable MongoDB error in %s (%s) — will retry: %s',
            _current_op_name(), type(exc).__name__, exc)
        return True
    return False


# Shared retry decorator for any MongoDB operation (reads and writes).
# Exponential backoff: 2 s → 4 s → 8 s → … capped at 30 s.
_mongo_retry = retry(
    retry_on_exception=_retry_on_mongo_error,
    wait_exponential_multiplier=2000,
    wait_exponential_max=30000,
    stop_max_attempt_number=10,
)


def retry_mongo_operation(func):
    """Decorator: retry a MongoDB operation on transient errors and tag
    any retry log entries with the wrapped function's qualified name so
    failures can be attributed to a specific call site."""
    op_name = getattr(func, '__qualname__', None) or func.__name__

    # The retry-decorated call must be the inner layer so that the
    # operation-name context is still set when the retry predicate runs.
    # Restoring it in the outer wrapper's finally happens only after all
    # retry attempts have either succeeded or finally failed.
    @_mongo_retry
    def _retried_call(*args, **kwargs):
        return func(*args, **kwargs)

    @wraps(func)
    def wrapper(*args, **kwargs):
        previous = getattr(_op_context, 'name', None)
        _op_context.name = op_name
        try:
            return _retried_call(*args, **kwargs)
        finally:
            _op_context.name = previous
    return wrapper


@_mongo_retry
def _retry_mongo_call(method, *args, **kwargs):
    return method(*args, **kwargs)


def retry_mongo_upsert(method, *args, **kwargs):
    """Retry a bound MongoDB call (e.g. ``collection.bulk_write``) on
    transient errors. Tags retry log entries with the bound method's
    qualified name when available."""
    previous = getattr(_op_context, 'name', None)
    _op_context.name = (
        getattr(method, '__qualname__', None)
        or getattr(method, '__name__', None)
        or 'retry_mongo_upsert'
    )
    try:
        return _retry_mongo_call(method, *args, **kwargs)
    finally:
        _op_context.name = previous


def get_month_start(date, timezone=None):
    date = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if timezone:
        date = date.replace(tzinfo=timezone.utc)
    return date


def bytes_to_gb(value):
    return value / 2**30


def retry_backoff(exc_class, tries=8, delay=2, backoff=2, raise_errors=None,
                  raise_codes=None):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            m_tries, m_delay = tries, delay
            while m_tries > 1:
                try:
                    return f(*args, **kwargs)
                except exc_class as e:
                    if raise_errors and type(e) in raise_errors:
                        raise
                    if raise_codes and getattr(
                            e, 'status_code', None) in raise_codes:
                        raise
                    time.sleep(m_delay)
                    m_tries -= 1
                    m_delay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

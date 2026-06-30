from pymongo import MongoClient


DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 30000
DEFAULT_CONNECT_TIMEOUT_MS = 20000
DEFAULT_SOCKET_TIMEOUT_MS = 600000
DEFAULT_MAX_POOL_SIZE = 50
DEFAULT_MIN_POOL_SIZE = 10
DEFAULT_MAX_IDLE_TIME_MS = 300000


def get_mongo_client(
    connection_url,
    server_selection_timeout_ms=DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
    connect_timeout_ms=DEFAULT_CONNECT_TIMEOUT_MS,
    socket_timeout_ms=DEFAULT_SOCKET_TIMEOUT_MS,
    max_pool_size=DEFAULT_MAX_POOL_SIZE,
    min_pool_size=DEFAULT_MIN_POOL_SIZE,
    max_idle_time_ms=DEFAULT_MAX_IDLE_TIME_MS,
    retry_writes=True,
    retry_reads=True,
    **extra_kwargs
):
    return MongoClient(
        connection_url,
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
        socketTimeoutMS=socket_timeout_ms,
        maxPoolSize=max_pool_size,
        minPoolSize=min_pool_size,
        maxIdleTimeMS=max_idle_time_ms,
        retryWrites=retry_writes,
        retryReads=retry_reads,
        **extra_kwargs
    )

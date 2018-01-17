import recommendations.config as config
from pl_cache import RedisCache

def get_cache():
    return RedisCache(
        'recommendations_etl_cache',
        redis_host=config.REDIS_HOST,
        redis_port=config.REDIS_PORT,
        redis_db=config.REDIS_RECOMMENDATIONS_ETL_DB,
        cache_default_timeout=config.REDIS_DEFAULT_TIMEOUT
    )

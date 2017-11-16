
import json
import os

# Load the config file, if it exists.
_file = os.getenv('PL_ENV_FILE', "/opt/pulselocker/config/recommendations-etl_env.json")

try:
    with open(_file) as config_file:
        _config_json = json.load(config_file)
except IOError:
    _config_json = {}
except:
    raise


def _get(name, default=None):
    """
    Return the value of a config variable. Priority is given to an environment
    variable. If it doesn't exist, the loaded config JSON is checked. Finally,
    it is set to `default` parameter if neither contain a value.
    """
    return os.getenv(name, _config_json.get(name, default))

ENVIRONMENT = _get('ENVIRONMENT', "test")

# Special Datamart stuff
POSTGRES_DATAMART_DB = _get("POSTGRES_DATAMART_DB", "metrics")
POSTGRES_DATAMART_HOST = _get("POSTGRES_DATAMART_HOST", "localhost")
POSTGRES_DATAMART_PORT =_get("POSTGRES_DATAMART_PORT", 5432)
POSTGRES_DATAMART_USER =  _get("POSTGRES_DATAMART_USER", "plpg")
POSTGRES_PULSELOCKER_PASSWORD = _get("POSTGRES_PULSELOCKER_PASSWORD", None)

REDIS_HOST = _get('REDIS_HOST', 'localhost')
REDIS_PORT = _get('REDIS_PORT', 6379)
REDIS_RECOMMENDATIONS_ETL_DB = _get('REDIS_SUBSCRIPTION_DB', 11)
REDIS_DEFAULT_TIMEOUT = _get('REDIS_DEFAULT_TIMEOUT', 60)

# Celery and Rabbit config
RABBIT_HOST = _get('RABBIT_HOST', "localhost")
RABBIT_PASSWORD = _get('RABBIT_PASSWORD', "guest")
RABBIT_PORT = _get('RABBIT_PORT', 15672)
RABBIT_USER = _get('RABBIT_USER', "guest")
BEAT_TASK_RETRIES = _get('BEAT_TASK_RETRIES', 10)
BEAT_TASK_RETRY_DELAY = _get('BEAT_TASK_RETRY_DELAY', 120)
TASK_RETRIES = _get('TASK_RETRIES', 5)
TASK_RETRY_DELAY = _get('TASK_RETRY_DELAY', 240)
CELERY_PREFETCH_MULTIPLIER = _get('CELERY_PREFETCH_MULTIPLIER', 1)
CELERY_WORKERS = _get('CELERY_WORKERS', 2)
CELERY_LOG_PATH = _get('CELERY_LOG_PATH', "/var/log/celery/")

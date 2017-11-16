#
# CONFIDENTIAL
#
# This source code is proprietary, confidential information of Pulselocker, Inc.
# It contains Pulselocker intellectual property, including trade secrets and
# copyright-protected authorship, and may include patentable inventions. You may
# not distribute this source code outside of Pulselocker without express written
# permission from management. Pulselocker does not claim ownership of included
# open source software components, which are subject to their own licenses.
#

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

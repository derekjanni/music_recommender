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
from pl_postgres import PostgreSQL

def get_datamart_connection(dict_cursor=True):
    """Return a psycopg2 Connection instance to `crunch_mart` Database."""
    return PostgreSQL(
        dbname=config.POSTGRES_DATAMART_DB,
        dbhost=config.POSTGRES_DATAMART_HOST,
        dbport=config.POSTGRES_DATAMART_PORT,
        dbuser=config.POSTGRES_DATAMART_USER,
        dbpass=config.POSTGRES_PULSELOCKER_PASSWORD,
        dict_cursor=dict_cursor
    ).get_connection()

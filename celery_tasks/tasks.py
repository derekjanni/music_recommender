import kombu
import newrelic.agent
import pika
import psycopg2
import sys
import recommendations.config as config
import traceback
from celery import Celery
from celery.schedules import crontab
import recommendations.celery_tasks.log as log

logger = log.get_file_logger('worker.log')

class CeleryConfiguration(object):
    """An object wrapper for celery configuration when calling `app.config_from_object`."""

    BROKER_URL = 'amqp://%(user)s:%(password)s@%(host)s' % {
        'user': config.RABBIT_USER,
        'password': config.RABBIT_PASSWORD,
        'host': config.RABBIT_HOST
    }

    CELERYBEAT_SCHEDULE = {
        'generate_artist_to_artist_recommendations': {
            'task': 'recommendations.celery_tasks.als.recommendation_builder.process_recommendations',
            'schedule': crontab(minute=0, hour=0, day_of_week='monday'),
            'args': ({'entity_type': 'artist', 'threshold': 5},)
        },
        'generate_label_to_label_recommendations': {
            'task': 'recommendations.celery_tasks.als.recommendation_builder.process_recommendations',
            'schedule': crontab(minute=0, hour=2, day_of_week='monday'),
            'args': ({'entity_type': 'label', 'threshold': 5},)
        },
        'generate_release_to_release_recommendations': {
            'task': 'recommendations.celery_tasks.als.recommendation_builder.process_recommendations',
            'schedule': crontab(minute=0, hour=4, day_of_week='monday'),
            'args': ({'entity_type': 'release', 'threshold': 5},)
        },
        'generate_song_to_song_recommendations': {
            'task': 'recommendations.celery_tasks.als.recommendation_builder.process_recommendations',
            'schedule': crontab(minute=0, hour=6, day_of_week='monday'),
            'args': ({'entity_type': 'song', 'threshold': 5},)
        }
    }


    CELERY_TASK_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'json'
    CELERY_ACCEPT_CONTENT = ['json']
    CELERY_ENABLE_UTC = True
    CELERY_DEFAULT_QUEUE = 'unspecified'
    CELERY_QUEUES = (
        kombu.Queue('implicit_als', kombu.Exchange(''), routing_key='implicit_als'),
        kombu.Queue('write_tasks', kombu.Exchange(''), routing_key='write_events')
    )


    CELERYD_CONCURRENCY = config.CELERY_WORKERS
    CELERYD_PREFETCH_MULTIPLIER = config.CELERY_PREFETCH_MULTIPLIER

    CELERY_IMPORTS = (
        'recommendations.celery_tasks.log',
        'recommendations.celery_tasks.als.recommendation_builder',
    )

    def __init__(self):
        pass


def handle_exception(exception, logger=None):
    """
    Handles a task exception and returns if it's recoverable and the exception in a tuple.
    Records all exceptions in New Relic.
    """
    exception_msg = sys.exc_info()

    try:
        newrelic.agent.record_exception(*exception_msg)
    except Exception as e:
        if logger:
            logger.warn("Failed to record in New Relic: {0}".format(e))

    if (
        isinstance(exception, psycopg2.OperationalError) or
        isinstance(exception, psycopg2.InternalError)
    ):
        if logger:
            logger.error("Postgres error: {0}, retrying...".format(exception))
        return (True, exception)

    elif isinstance(exception, pika.exceptions.AMQPConnectionError):
        if logger:
            logger.error("MQ error: {0}, retrying...".format(exception))
        return (True, exception)

    else:
        if logger:
            logger.error("Undefined exception: {0}".format(exception))
            logger.error(traceback.format_exc())
        # TODO: Undefined exceptions should be sent to dead letter exchange.
        return (False, exception)


# Celery initialization
app = Celery()
app.config_from_object(CeleryConfiguration())

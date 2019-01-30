# Custom work based on work done here: https://github.com/benfred/implicit/blob/master/examples/lastfm.py
# and here: https://bugra.github.io/work/notes/2014-04-19/alternating-least-squares-method-for-collaborative-filtering/

import recommendations.database.cache as cache_factory
import recommendations.database.postgres as recommendations_pg
import recommendations.celery_tasks.tasks as tasks
import recommendations.celery_tasks.log as logging
import pandas as pd
import numpy as np
import pickle
import time
from scipy.sparse import coo_matrix
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import bm25_weight


cache = cache_factory.get_cache()

log = logging.get_file_logger('implicit_als.log')

_ENTITY_TYPES = {'artist', 'song', 'release', 'label'}

_SQL = {
    'get_entity_training_data':"""
        select
            user_id,
            {0}_id,
            count
        from (
            select
                user_id,
                {0}_id,
                count(*)
            from fact_tables.usage_facts
            group by user_id, {0}_id
        ) as t
        where count > %(threshold)s
        and {0}_id is not null
        group by
            t.user_id,
            t.{0}_id,
            t.count
    """
}

def train_model(
        df,
        plays,
        entity_type,
        factors=100,
        regularization=0.03,
        iterations=30,
        use_native=True,
        dtype=np.float64,
        use_cg=False
    ):
    log.info("Calculating similar %ss. This might take a while" % entity_type)
    entity_id_name = "%s_id" % entity_type
    model_name = 'ALS'

    model = AlternatingLeastSquares(
        factors=factors,
        regularization=regularization,
        use_native=use_native,
        use_cg=use_cg,
        dtype=dtype,
        iterations=iterations
    )

    # train the model
    log.info("training model %s" % model_name)
    start = time.time()
    model.fit(plays)
    log.info("trained model '%s' in %s" % (model_name, time.time() - start))

    # write out similar artists by popularity
    log.info("calculating top %ss" % entity_type)
    user_count = df.groupby(entity_id_name).size()
    entities = dict(enumerate(df[entity_id_name].cat.categories))
    to_generate = list(x for x in sorted(list(entities), key=lambda x: -user_count[x]))

    # write out as a TSV of artistid, otherartistid, score
    for entity_id in to_generate:
        recommendations = {x[0]: x[1] for x in model.similar_items(entity_id, 11)}
        recommendations.pop(entity_id, None)
        write.delay({
            'entity_type': entity_type,
            'entity_id': entity_id,
            'recommendations': {str(x): y for x,y in recommendations.items()}
        })


@tasks.app.task(queue='implicit_als')
def process_recommendations(payload):
    entity_type = payload.get('entity_type')
    threshold = payload.get('threshold', 5)
    entity_id = "%s_id" % entity_type
    df = pd.read_sql(
        _SQL['get_entity_training_data'].format(entity_type),
        recommendations_pg.get_datamart_connection(),
        params={'threshold': threshold}
    )
    df['user_id'] = df['user_id'].astype("category")
    df[entity_id] = df[entity_id].astype("category")
    plays = coo_matrix(
        (
            df['count'].astype(float),
            (
                df[entity_id].cat.codes.copy(),
                df['user_id'].cat.codes.copy()
            )
        )
    )

    train_model(df, plays, entity_type)


@tasks.app.task(queue='implicit_als')
def write(payload):
    """
    Stores data in Redis in pickled form
    """
    entity_type = payload.get('entity_type')
    entity_id = payload.get('entity_id')
    recommendations = payload.get('recommendations')
    client = cache.get_connection()
    key = "{}:{}:{}".format(
        cache.cache_name,
        entity_type,
        entity_id
    )
    data = pickle.dumps(recommendations)
    log.info('Writing recommendations to Redis for %s %s' % (entity_type, entity_id))
    client.setex(key, data, cache.cache_default_timeout)


def check_cache(entity_type, entity_id):
    """
    Debug method to ensure that data is getting loaded to the cache
    """
    client = cache.get_connection()
    key = "{}:{}:{}".format(
        cache.cache_name,
        entity_type,
        entity_id
    )
    recommendations = client.get(key)
    return pickle.loads(recommendations) if recommendations else None


# Custom work based on work done here: https://github.com/benfred/implicit/blob/master/examples/lastfm.py
# and here: https://bugra.github.io/work/notes/2014-04-19/alternating-least-squares-method-for-collaborative-filtering/

import recommendations.database.postgres as recommendations_pg
import random
from pprint import pprint
import pandas as pd
import numpy as np
import cPickle
import argparse
import logging
import time
from tqdm import tqdm
from scipy.sparse import coo_matrix
import sys
from implicit.als import AlternatingLeastSquares
from implicit.nearest_neighbours import (BM25Recommender, CosineRecommender,
                                         TFIDFRecommender, bm25_weight)


_ENTITY_TYPES = {'artist', 'song', 'release', 'label'}

_SQL = {
    'get_user_artists':"""
        select artist_name, artist_id, count(*)
        from fact_tables.usage_facts where user_id=31813
          and artist_name is not null
          group by artist_name, artist_id
          order by count(*) desc;
    """,
    'get_entity_training_data':"""
        select
            user_id,
            {0},
            {1}_id,
            count
        from (
            select
                user_id,
                {0},
                {1}_id,
                count(*)
            from fact_tables.usage_facts
            where {0} is not null
            group by user_id, {0}, {1}_id
        ) as t
        where count > {2}
        group by
            t.user_id,
            t.{0},
            t.{1}_id,
            t.count
    """
}

ENTITY_NAMES = {
    'artist': 'artist_name',
    'release': 'release_title',
    'song': 'song_title',
    'label': 'label_name'
}

def train_model(
        df,
        plays,
        entity_type,
        model_name="als",
        factors=100, regularization=0.03,
        iterations=30,
        exact=True,
        use_native=True,
        dtype=np.float64,
        cg=False
    ):
    print "Calculating similar %ss. This might take a while" % entity_type
    entity_name = ENTITY_NAMES[entity_type]
    entity_id_name = "%s_id" % entity_type

    # generate a recommender model based off the input params
    if model_name == "als":
        if exact:
            model = AlternatingLeastSquares(factors=factors, regularization=regularization,
                                            use_native=use_native, use_cg=cg,
                                            dtype=dtype, iterations=iterations)
        else:
            model = AnnoyAlternatingLeastSquares(factors=factors, regularization=regularization,
                                                 use_native=use_native, use_cg=cg,
                                                 dtype=dtype, iterations=iterations)

        # lets weight these models by bm25weight.
        print "weighting matrix by bm25_weight"
        plays = bm25_weight(plays, K1=100, B=0.8)

    elif model_name == "tfidf":
        model = TFIDFRecommender()

    elif model_name == "cosine":
        model = CosineRecommender()

    elif model_name == "bm25":
        model = BM25Recommender(K1=100, B=0.5)

    else:
        raise NotImplementedError("TODO: model %s" % model_name)

    # train the model
    print "training model %s" % model_name
    start = time.time()
    model.fit(plays)
    print "trained model '%s' in %s" % (model_name, time.time() - start)

    # write out similar artists by popularity
    print "calculating top %ss" % entity_type
    user_count = df.groupby(entity_id_name).size()
    entities = dict(enumerate(df[entity_id_name].cat.categories))
    to_generate = list(x for x in sorted(list(entities), key=lambda x: -user_count[x]))

    # write out as a TSV of artistid, otherartistid, score
    rec_dict = {}
    for entity_id in tqdm(to_generate):
        recommendation_subject = df[df[entity_id_name]==entities[entity_id]][entity_name].values[0]
        recommendations = {df[df[entity_id_name]==entities[x[0]]][entity_name].values[0]: x[1] for x in model.similar_items(entity_id, 11)}
        recommendations.pop(recommendation_subject, None)
        rec_dict[recommendation_subject.lower()]= recommendations

    return rec_dict


def get_recommendations(entity_type, threshold, use_pickle=True):
    entity_name = ENTITY_NAMES[entity_type]
    entity_id = "%s_id" % entity_type

    if use_pickle:
        df =  pd.read_pickle('%s_df_implicit.pkl' % entity_type)
        with open('%s_plays.pkl' % entity_type) as f:
            plays = cPickle.load(f)

    else:
        df = pd.read_sql(
            _SQL['get_entity_training_data'].format(entity_name, entity_type, threshold),
            recommendations_pg.get_datamart_connection()
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

        df.to_pickle('%s_df_implicit.pkl' % entity_type)
        with open('%s_plays.pkl' % entity_type, 'wb') as f:
            cPickle.dump(plays, f)

    recommendations = train_model(df, plays, entity_type)
    with open('%s_recommender.pkl' % entity_type, 'wb') as f:
        cPickle.dump(recommendations, f)

if __name__=="__main__":
    get_recommendations(sys.argv[1], sys.argv[2])

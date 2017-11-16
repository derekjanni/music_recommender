import unittest
import mock
import pandas as pd
import numpy as np
import recommendations.celery_tasks.als.recommendation_builder as recommendation_builder

class RecommendationBuilderTest(unittest.TestCase):

    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.write')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.AlternatingLeastSquares')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.log')
    def test_train_model(self, log_mock, model_mock, write_mock):
        df_mock = pd.DataFrame({'user_id': [1], 'release_id': [2], 'count': [2]})
        plays_mock = mock.Mock()
        df_mock['user_id'] = df_mock['user_id'].astype("category")
        df_mock['release_id'] = df_mock['release_id'].astype("category")
        model_mock.similar_items.return_value = [{'x0': 1, 'x1': 2}, {'x0': 1, 'x1': 2}, {'x0': 1, 'x1': 2}]
        recommendation_builder.train_model(
            df_mock,
            plays_mock,
            'release',
            factors=100,
            regularization=0.03,
            iterations=30,
            use_native=True,
            dtype=np.float64,
            use_cg=False
        )
        write_mock.delay.assert_called_with(
            {
                'entity_type': 'release',
                'entity_id': 0,
                'recommendations': {}
            }
        )


    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.train_model')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.coo_matrix')
    @mock.patch('recommendations.database.postgres.get_datamart_connection')
    @mock.patch('pandas.read_sql')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.log')
    def test_process_recommendations(
        self,
        log_mock,
        read_sql_mock,
        get_pg_connection_mock,
        coo_matrix_mock,
        train_model_mock
    ):
        df_mock = pd.DataFrame({'user_id': [1], 'release_id': [2], 'count': [2]})
        read_sql_mock.return_value = df_mock
        plays_mock = mock.Mock()
        coo_matrix_mock.return_value = plays_mock
        recommendation_builder.process_recommendations({'entity_type': 'release', 'threshold': 5})
        train_model_mock.assert_called_with(df_mock, plays_mock, 'release')

    @mock.patch('pickle.dumps')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.cache')
    @mock.patch('recommendations.celery_tasks.als.recommendation_builder.log')
    def test_write(self, log_mock, cache_mock, pickle_mock):
        client_mock = mock.Mock()
        cache_mock.get_connection.return_value = client_mock
        cache_mock.cache_name = 'recommendations_etl'
        cache_mock.cache_default_timeout = 60
        recommendations_mock = mock.Mock()
        pickle_mock.return_value = recommendations_mock
        recommendation_builder.write({'entity_type': 'release', 'entity_id': 10280, 'recommendations': recommendations_mock})
        cache_mock.get_connection.assert_called_with()
        pickle_mock.assert_called_with(recommendations_mock)
        client_mock.setex.assert_called_with('recommendations_etl:release:10280', recommendations_mock, 60)


if __name__ == '__main__':
    unittest.main()

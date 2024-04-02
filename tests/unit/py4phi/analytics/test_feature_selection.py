import numpy as np
import pandas as pd
import pytest

from py4phi.analytics.feature_selection import FeatureSelection


@pytest.fixture()
def feature_selection_obj():

    return FeatureSelection(
        df=pd.DataFrame.from_dict({
            'col1': [i for i in range(1, 101)],
            'col2': [10 * y for y in (1, 2) for _ in range(50)],
            'col3': ['a' for _ in range(100)],
            'col4': [char for char in ('c', 'd') for _ in range(50)],
            'target': [char for char in ('yes', 'no') for _ in range(50)]
        }),
        target_column='target'
    )


@pytest.fixture()
def crosstab_matrix():
    return pd.DataFrame.from_dict({
        'first_a': [2, 2],
        'first_b': [2, 2],
        'second_a': [30, 0],
        'second_b': [0, 30],
        'third_a': [40, 20],
        'third_b': [20, 40]
    })


@pytest.fixture()
def correlation_matrix():
    matrix = pd.DataFrame.from_dict({
        'A': [1.0, 0.6, 0.3, 0.0],
        'B': [0.6, 1.0, 0.7, 0.4],
        'C': [0.3, 0.7, 1.0, 1.0],
        'D': [0.0, 0.7, 1.0, 1.0]
    })
    matrix.index = matrix.columns
    return matrix


@pytest.mark.parametrize('crosstab_columns,expected_value', [
    (['first_a', 'first_a'], .0),
    (['second_a', 'second_b'], 1.0),
    (['third_a', 'third_b'], .33)

])
def test_cramers_v(feature_selection_obj, crosstab_matrix,
                   crosstab_columns, expected_value):
    confusion_matrix = crosstab_matrix[crosstab_columns]

    res = feature_selection_obj.cramers_v(confusion_matrix)
    tolerance = 5e-2
    assert np.isclose(res, expected_value, atol=tolerance)


@pytest.mark.parametrize('threshold,additional_features_list', [
    (.5,  ['D']),
    (1.0, []),
    (0.0, ['C']),
])
def test_find_correlated_pairs(feature_selection_obj, correlation_matrix,
                               threshold, additional_features_list):
    actual_recommendations = feature_selection_obj.find_correlated_pairs(
        ['B'],
        correlation_matrix,
        threshold
    )

    assert actual_recommendations == ['B'] + additional_features_list


@pytest.mark.parametrize('override_columns,to_drop', [
    ([], ['col3', 'col2']),
    (['col1', 'col4'], ['col1', 'col4']),
    (['col1'], ['col1'])
])
def test_correlation_analysis(feature_selection_obj, override_columns, to_drop):
    expected_result = feature_selection_obj._df.copy().drop(to_drop, axis=1)
    result = feature_selection_obj.correlation_analysis(
        0.5,
        0.5,
        override_columns_to_drop=override_columns,
        drop_recommended=True
    )
    pd.testing.assert_frame_equal(result, expected_result)


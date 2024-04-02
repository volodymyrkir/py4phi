import re

import pytest
import numpy as np
import pandas as pd

from py4phi.analytics.principal_component_analysis import PrincipalComponentAnalysis


@pytest.fixture()
def pca_obj():
    return PrincipalComponentAnalysis(
        df=pd.DataFrame.from_dict({
           'col1': [np.nan, None, 0, 1, 5],
           'col2': [np.nan, np.nan, np.nan, 'str1', 'str2'],
           'col3': [1, 2, 3, 4, 5],
           'col4': [5 for _ in range(5)],
           'target': ['class1', 'class2', 'class1', 'class2', 'class1']
        }),
        target_column='target'
    )


def test_successful_init(pca_obj):
    assert all(attribute is not None
               for attribute in (pca_obj.scaler, pca_obj._df, pca_obj._target_column))


def test_handle_nulls_fill_mode(pca_obj):
    input_df = pca_obj._df.drop('col2', axis=1)
    actual_df = pca_obj.handle_nulls(
        input_df,
        mode='fill'
    )
    input_df['col1'] = [2.5, 1.0, 0, 1, 5]

    assert actual_df.isna().sum().sum() == 0
    pd.testing.assert_frame_equal(actual_df, input_df)


def test_handle_nulls_fill_mode_no_target(pca_obj):
    pca_obj._target_column = None
    input_df = pca_obj._df.drop(['col2', 'target'], axis=1)
    actual_df = pca_obj.handle_nulls(
        input_df,
        mode='fill'
    )
    input_df['col1'] = [2.0, 2.0, 0, 1, 5]

    assert actual_df.isna().sum().sum() == 0
    pd.testing.assert_frame_equal(actual_df, input_df)


def test_handle_nulls_drop_mode(pca_obj):
    actual_df = pca_obj.handle_nulls(pca_obj._df, mode='drop')

    assert actual_df.isna().sum().sum() == 0
    pd.testing.assert_frame_equal(actual_df, pca_obj._df.iloc[3:])


def test_handle_nulls_wrong_mode(pca_obj):
    with pytest.raises(ValueError):
        pca_obj.handle_nulls(pca_obj._df, mode='else')


@pytest.mark.parametrize('columns_to_join,components_len', [
    (['col4', 'target'], 2),
    (['target'], 3),
    ([], 3)
])
def test_reduce_features(pca_obj, columns_to_join, components_len):

    scaled_data = np.random.rand(len(pca_obj._df), components_len)

    result = pca_obj._reduce_features(components_len, scaled_data, columns_to_join)

    assert result.shape == (len(pca_obj._df), (len(columns_to_join) + components_len))
    assert all(f'PC{i}' in result.columns for i in range(1, components_len+1))


def test_component_analysis_string_col(pca_obj):
    with pytest.raises(TypeError):
        pca_obj.component_analysis()


def test_component_analysis_ignored_all(pca_obj):
    with pytest.raises(ValueError):
        pca_obj.component_analysis(ignore_columns=['col1', 'col2', 'col3', 'col4'])


def test_component_analysis_provided_too_many_components(mocker, pca_obj):
    mock_scaler = mocker.patch.object(pca_obj, 'scaler')

    with pytest.raises(ValueError):
        pca_obj.component_analysis(ignore_columns=['col2'], n_components=len(pca_obj._df))
    assert mock_scaler.fit_transform.called


@pytest.mark.parametrize('threshold', [
    0.5, 0.3, 0.95, 1.0
])
def test_component_analysis_no_reduce(mocker, pca_obj, threshold):
    mock_logger = mocker.patch('py4phi.analytics.principal_component_analysis.logger')
    result = pca_obj.component_analysis(rec_threshold=threshold, ignore_columns=['col2'])
    last_log = mock_logger.info.call_args_list[-1][0][0]
    pattern = r'Explained variance suggests keeping at least (\d+) components \((\d+\.\d+% cumulative variance)\)\.'

    assert result is None
    assert re.fullmatch(pattern, last_log)


def test_component_analysis_default(pca_obj):
    result = pca_obj.component_analysis(reduce_features=True, ignore_columns=['col2'])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(pca_obj._df)
    assert 2 < len(result.columns) < len(pca_obj._df.columns)


@pytest.mark.parametrize('ignore,n_components,expected_length', [
    (['col2'], 2, 4),
    (['col2', 'col1'], 1, 4),
])
def test_component_analysis_override(pca_obj, ignore, n_components, expected_length):
    result = pca_obj.component_analysis(
        reduce_features=True, ignore_columns=ignore, n_components=n_components
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(pca_obj._df)
    assert len(result.columns) == expected_length

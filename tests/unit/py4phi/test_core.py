import os
from pathlib import Path
from secrets import token_hex

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pyspark.sql import DataFrame
from pyspark.sql import types as t

from py4phi.consts import DEFAULT_MODEL_KEY_PATH
from py4phi.core import decrypt_model, encrypt_model, from_dataframe, from_path
from py4phi.dataset_handlers.pandas_dataset_handler import PandasDatasetHandler
from py4phi.dataset_handlers.polars_dataset_handler import PolarsDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler


def test_from_path_wrong_engine():
    with pytest.raises(ValueError):
        from_path('some_path', 'some_type', engine='RANDOMENGINE')


@pytest.mark.parametrize('engine,expected_reader,expected_df_type', [
    ('pyspark', PySparkDatasetHandler, DataFrame),
    ('pandas', PandasDatasetHandler, pd.DataFrame),
    ('polars', PolarsDatasetHandler, pl.DataFrame)
])
def test_from_path_engine(engine, expected_reader, tmp_path, expected_df_type):
    file_path = tmp_path / 'file.csv'
    with open(file_path, 'w') as f:
        f.write('A,B,C,D\n'
                '1,2,3,4\n'
                '5,6,7,8\n'
                '9,10,11,12\n')

    controller = from_path(str(file_path), file_type='csv', engine=engine)
    assert isinstance(controller._current_df, expected_df_type)
    assert isinstance(controller._dataset_handler, expected_reader)


def test_from_dataframe_non_supported():
    with pytest.raises(ValueError):
        from_dataframe(np.array([1, 2, 3, 4]))


@pytest.mark.parametrize('input_df,expected_handler', [
    (pd.DataFrame(), PandasDatasetHandler),
    (pl.DataFrame(), PolarsDatasetHandler)
])
def test_from_path_pandas_polars(input_df, expected_handler):
    controller = from_dataframe(input_df)

    assert isinstance(controller._dataset_handler, expected_handler)
    assert controller._current_df is input_df


def test_from_path_pyspark(spark_session):
    df = spark_session.createDataFrame([1, 2, 3], t.IntegerType())
    controller = from_dataframe(df)

    assert isinstance(controller._dataset_handler, PySparkDatasetHandler)
    assert controller._current_df is df


@pytest.mark.parametrize('encrypt_config,expected_files', [
    (True, ['secret.key', 'decrypt.conf']),
    (False, ['decrypt.conf'])
])
def test_encrypt_model(tmp_path, encrypt_config, expected_files):
    folder = tmp_path / "test_folder"
    folder.mkdir()
    encrypt_model(folder, encrypt_config=encrypt_config)
    assert os.listdir(tmp_path / DEFAULT_MODEL_KEY_PATH) == expected_files


def test_decrypt_model_successful(mocker):
    mock_read_config = mocker.patch('py4phi.core.ConfigProcessor.read_config')
    mock_model_encryptor = mocker.patch('py4phi.core.ModelEncryptor')
    mock_read_config.return_value = {"model": {"key": "test_key", "aad": "test_aad"}}
    path = Path("test_path")

    decrypt_model(path)

    mock_read_config.assert_called_once_with(
        os.path.join(path.parent.absolute(), DEFAULT_MODEL_KEY_PATH),
        config_encrypted=True
    )
    mock_model_encryptor.decrypt_folder.assert_called_once_with(
        path, "test_key", "test_aad"
    )


def test_decrypt_model_missing_keys(mocker):
    mocker.patch('py4phi.core.ConfigProcessor.read_config', return_value={})
    path = Path("test_path")

    with pytest.raises(ValueError):
        decrypt_model(path)


def test_decrypt_model_config_not_encrypted(mocker):
    mock_read = mocker.patch(
        'py4phi.core.ConfigProcessor.read_config',
        return_value={
            "model": {
                "key": token_hex(16),
                "aad": token_hex(16)
            }
        }
    )
    path = Path("test_path")

    decrypt_model(path, config_encrypted=False)

    mock_read.assert_called_once_with(
        os.path.join(path.parent.absolute(), DEFAULT_MODEL_KEY_PATH),
        config_encrypted=False
    )

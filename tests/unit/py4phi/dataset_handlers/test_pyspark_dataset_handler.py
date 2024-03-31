"""Contains tests for the PySparkDatasetHandler class."""
import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler


@pytest.fixture()
def handler():
    return PySparkDatasetHandler()


def test_init(handler):
    assert isinstance(handler, BaseDatasetHandler)
    assert isinstance(handler._spark, SparkSession)


@pytest.mark.parametrize('file_type', [
    'parquet', 'csv',
])
def test_read_file(handler, mocker, mock_logger, file_type):
    df = mocker.MagicMock()
    mock_read_csv = mocker.MagicMock(return_value=df)
    mock_read_csv.__name__ = '_read_csv'

    mocker.patch(f'py4phi.dataset_handlers.pyspark_dataset_handler.PySparkDatasetHandler._read_{file_type}',
                 mock_read_csv)

    handler.read_file(f'test/path.{file_type}', file_type, header=True, param1=123)
    mock_read_csv.assert_called_once_with(f'test/path.{file_type}',
                                          header=True, param1=123)


def test_write_default(handler, mocker):
    write_method_mock = mocker.MagicMock()
    mocker.patch.object(handler, '_write_csv', write_method_mock)
    handler.write('df', 'name', 'path', param1='param1', save_format=None)
    write_method_mock.assert_called_once_with('df', 'name', 'path', param1='param1')


@pytest.mark.parametrize('file_type', [
    'parquet', 'csv',
])
def test_write_others(handler, mocker, file_type):
    write_method_mock = mocker.MagicMock()
    write_method_mock.__name__ = f'_write_{file_type}'
    mocker.patch(f'py4phi.dataset_handlers.pyspark_dataset_handler.PySparkDatasetHandler._write_{file_type}',
                 write_method_mock)
    _, handler._writing_method = handler._get_interaction_methods(file_type)
    handler.write('df', 'name', 'path', param1='param1', save_format=file_type)
    write_method_mock.assert_called_once_with('df', 'name', 'path', param1='param1')


def test_print(handler, mocker):
    df = mocker.MagicMock(spec=DataFrame)
    show_mock = mocker.MagicMock()
    df.show = show_mock
    handler.print_df(df)
    show_mock.assert_called_once()


def test_print_raise(handler, mocker):
    df = mocker.MagicMock()
    with pytest.raises(ValueError):
        handler.print_df(df)


def test_to_pandas(handler, spark_session):
    handler._df = spark_session.createDataFrame([
        [1, 2], [3, 4]
    ], schema=['col1', 'col2'])

    res = handler.to_pandas()

    pd.testing.assert_frame_equal(res, pd.DataFrame.from_dict({
        'col1': [1, 3],
        'col2': [2, 4]
    }))

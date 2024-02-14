"""Contains tests for the PySparkDatasetHandler class."""
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


def test_read_file(handler, mocker, mock_logger):
    df = mocker.MagicMock()
    mock_read_csv = mocker.MagicMock(return_value=df)
    mock_read_csv.__name__ = '_read_csv'

    mocker.patch('py4phi.dataset_handlers.pyspark_dataset_handler.PySparkDatasetHandler._read_csv',
                 mock_read_csv)

    handler.read_file('test/path.csv', 'csv', header=True, param1=123)
    mock_read_csv.assert_called_once_with('test/path.csv', header=True, param1=123)


def test_write_default(handler, mocker):
    write_method_mock = mocker.MagicMock()
    mocker.patch.object(handler, '_write_csv', write_method_mock)
    handler.write('df', 'name', 'path', param1='param1')
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
    handler.write('df', 'name', 'path', param1='param1')
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

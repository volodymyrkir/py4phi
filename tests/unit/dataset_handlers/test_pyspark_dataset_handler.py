"""Contains tests for the PySparkDatasetHandler class."""
import pytest
from pyspark.sql import DataFrame

from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler


@pytest.fixture()
def handler():
    return PySparkDatasetHandler()


def test_read_dataframe(mocker, handler):
    fake_df = mocker.MagicMock()
    handler.read_dataframe(fake_df)
    assert handler.df is fake_df


@pytest.mark.parametrize('file_type,funcs', [
    ('csv', ('_read_csv', '_write_csv')),
    ('cSv', ('_read_csv', '_write_csv')),
    ('CSV', ('_read_csv', '_write_csv')),
    ('parquet', ('_read_parquet', '_write_parquet')),
    ('PaRqUeT', ('_read_parquet', '_write_parquet'))
])
def test_get_interaction_methods_correct(handler, file_type, funcs):
    methods = handler._get_interaction_methods(file_type)
    assert tuple(map(lambda x: x.__name__, methods)) == funcs


@pytest.mark.parametrize('file_type', [
   'json', 'orc', 'enigma', 'iceberg'
])
def test_get_interaction_methods_raise(handler, file_type):
    with pytest.raises(NotImplementedError):
        handler._get_interaction_methods(file_type)


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
    mocker.patch('py4phi.dataset_handlers.pyspark_dataset_handler.PySparkDatasetHandler._write_csv',
                 write_method_mock)
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

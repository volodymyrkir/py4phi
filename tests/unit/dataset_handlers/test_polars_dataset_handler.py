"""Contains tests for the PolarsDatasetHandler class."""
import pytest
from polars import DataFrame

from py4phi.dataset_handlers.polars_dataset_handler import PolarsDatasetHandler


@pytest.fixture()
def handler():
    return PolarsDatasetHandler()


@pytest.mark.parametrize('file_type', [
    'csv', 'parquet'
])
def test_read_file(handler, mocker, mock_logger, file_type):
    df = mocker.MagicMock()
    mock_read_csv = mocker.MagicMock(return_value=df)
    mock_read_csv.__name__ = ''

    mocker.patch(f'py4phi.dataset_handlers.polars_dataset_handler.PolarsDatasetHandler._read_{file_type}',
                 mock_read_csv)

    handler.read_file(f'test/path.{file_type}', file_type, header=True, param1=123)
    mock_read_csv.assert_called_once_with(f'test/path.{file_type}',
                                          header=True, param1=123)


def test_write_default(handler, mocker):
    write_method_mock = mocker.MagicMock()
    mocker.patch('py4phi.dataset_handlers.polars_dataset_handler.PolarsDatasetHandler._write_csv',
                 write_method_mock)
    handler.write('df', 'name', 'path', param1='param1', save_format=None)
    write_method_mock.assert_called_once_with('df', 'name', 'path', param1='param1')


@pytest.mark.parametrize('file_type', [
    'parquet', 'csv',
])
def test_write_others(handler, mocker, file_type):
    write_method_mock = mocker.MagicMock()
    write_method_mock.__name__ = f'_write_{file_type}'
    mocker.patch(f'py4phi.dataset_handlers.polars_dataset_handler.PolarsDatasetHandler._write_{file_type}',
                 write_method_mock)
    _, handler._writing_method = handler._get_interaction_methods(file_type)
    handler.write('df', 'name', 'path', param1='param1', save_format=file_type)
    write_method_mock.assert_called_once_with('df', 'name', 'path', param1='param1')


def test_print(handler, mocker):
    df = mocker.MagicMock(spec=DataFrame)
    head_mock = mocker.MagicMock()
    df.head = head_mock
    handler.print_df(df)
    head_mock.assert_called_once()


def test_print_raise(handler, mocker):
    df = mocker.MagicMock()
    with pytest.raises(ValueError):
        handler.print_df(df)

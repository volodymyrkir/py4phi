import pandas as pd
import pytest

from py4phi._encryption._pandas_encryptor import _PandasEncryptor
from py4phi._encryption._polars_encryptor import _PolarsEncryptor
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor
from py4phi.config_processor import ConfigProcessor
from py4phi.consts import (
    DEFAULT_FEATURE_SELECTION_FOLDER_NAME,
    DEFAULT_PCA_REDUCED_FOLDER_NAME,
    DEFAULT_PY4PHI_DECRYPTED_NAME,
    DEFAULT_PY4PHI_ENCRYPTED_NAME,
)
from py4phi.controller import Controller
from py4phi.dataset_handlers.pandas_dataset_handler import PandasDatasetHandler
from py4phi.dataset_handlers.polars_dataset_handler import PolarsDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler


@pytest.fixture()
def controller():
    pandas_handler = PandasDatasetHandler()
    pandas_handler._df = pd.DataFrame.from_dict(
        {'col1': [1, 2, 3], 'col2': [2, 3, 4]}
    )
    return Controller(dataset_handler=pandas_handler)


@pytest.mark.parametrize('handler,expected_encryptor', [
    (PolarsDatasetHandler(), _PolarsEncryptor),
    (PandasDatasetHandler(), _PandasEncryptor),
    (PySparkDatasetHandler(), _PySparkEncryptor)
])
def test_successful_init(handler, expected_encryptor, mocker):
    mock_df = mocker.MagicMock()
    handler._df = mock_df

    controller = Controller(handler)

    assert controller._encryption_cls is expected_encryptor
    assert not any(uninitialized_field for uninitialized_field in
                   (controller._Controller__encryptor, controller._encrypted,
                    controller._decrypted, controller._Controller__columns_data))

    assert controller._current_df == mock_df
    assert isinstance(controller._config_processor, ConfigProcessor)


def test_print_current_df(controller, mocker):
    mock_print = mocker.patch.object(controller._dataset_handler, 'print_df')
    controller.print_current_df()

    mock_print.assert_called_once_with(controller._current_df)


@pytest.mark.parametrize('encryption_cls', [
    _PandasEncryptor,
    _PolarsEncryptor,
    _PySparkEncryptor
])
def test_encrypt_default(controller, mocker, encryption_cls):
    assert controller._Controller__encryptor is None

    controller._encryption_cls = encryption_cls
    encryption_cls.encrypt = mocker.MagicMock(
        side_effect=lambda: (controller._current_df, 'mapping')
    )

    result_df = controller.encrypt(['col'])

    assert isinstance(controller._Controller__encryptor, encryption_cls)
    encryption_cls.encrypt.assert_called_once()
    assert result_df is controller._current_df
    assert controller._encrypted


def test_encrypt_second_time(controller, mocker):
    mock_encryptor = mocker.patch.object(controller, '_Controller__encryptor')
    mock_encryptor.encrypt.side_effect = lambda: (controller._current_df, 'mapping')

    result_df = controller.encrypt(['col'])

    mock_encryptor.encrypt.assert_called_once()
    assert result_df is controller._current_df
    assert controller._encrypted


def test_save_encrypted_not_encrypted(controller):
    with pytest.raises(ValueError):
        controller.save_encrypted()


def test_save_encrypted(mocker, controller, tmp_path):
    kwargs = {'header': True, 'index': False}
    controller._encrypted = True
    mock_save_config = mocker.patch.object(controller._config_processor, 'save_config')
    mock_write = mocker.patch.object(controller._dataset_handler, 'write')

    controller.save_encrypted(
        config_file_name='myconf',
        key_file_name='mykey',
        save_location=tmp_path,
        save_format='parquet',
        **kwargs
    )

    mock_save_config.assert_called_once_with(
        controller._Controller__columns_data,
        path=str(tmp_path/DEFAULT_PY4PHI_ENCRYPTED_NAME),
        conf_file_name='myconf',
        encrypt_config=True,
        key_file_name='mykey'
    )
    mock_write.assert_called_once_with(
        controller._current_df,
        'output_dataset',
        str(tmp_path / DEFAULT_PY4PHI_ENCRYPTED_NAME),
        'parquet',
        header=True,
        index=False
    )


@pytest.mark.parametrize('decrypted', [True, False])
def test_save_decrypted_not_decrypted(controller, mocker, decrypted, tmp_path):
    kwargs = {'header': True, 'index': False}
    controller._decrypted = decrypted
    mock_warn = mocker.patch('py4phi.controller.logger.warn')
    mock_prepare_location = mocker.patch('py4phi.controller.prepare_location')
    mock_write = mocker.patch.object(controller._dataset_handler, 'write')

    controller.save_decrypted(save_location=tmp_path, **kwargs)
    mock_prepare_location.assert_called_once_with(
        location=str(tmp_path / DEFAULT_PY4PHI_DECRYPTED_NAME)
    )
    assert decrypted or mock_warn.called
    mock_write.assert_called_once_with(
        controller._current_df,
        'output_dataset',
        str(tmp_path / DEFAULT_PY4PHI_DECRYPTED_NAME),
        save_format='csv',
        header=True,
        index=False
    )


@pytest.mark.parametrize('encryption_cls', [
    _PandasEncryptor,
    _PolarsEncryptor,
    _PySparkEncryptor
])
def test_decrypt_encrypted(controller, mocker, encryption_cls):
    assert controller._Controller__encryptor is None
    assert controller._decrypted is False
    encryption_cls.decrypt = mocker.Mock(return_value=controller._current_df)
    controller._encryption_cls = encryption_cls
    controller._Controller__columns_data = {'key': 'val'}

    result_df = controller.decrypt(['col1'])

    assert isinstance(controller._Controller__encryptor, encryption_cls)
    assert result_df is controller._current_df
    encryption_cls.decrypt.assert_called_once_with({})
    assert controller._decrypted is True


@pytest.mark.parametrize('encryption_cls', [
    _PandasEncryptor,
    _PolarsEncryptor,
    _PySparkEncryptor
])
def test_decrypt_missing_configs(controller, mocker, encryption_cls):
    assert controller._decrypted is False
    controller._config_processor.read_config = mocker.Mock(
        side_effect=FileNotFoundError
    )
    controller._encryption_cls = encryption_cls

    with pytest.raises(FileNotFoundError):
        controller.decrypt(['col1'])
    assert controller._config_processor.read_config.called


@pytest.mark.parametrize('encryption_cls', [
    _PandasEncryptor,
    _PolarsEncryptor,
    _PySparkEncryptor
])
def test_decrypt_default(controller, mocker, encryption_cls, tmp_path):
    assert controller._decrypted is False
    controller._config_processor.read_config = mocker.Mock(
        return_value={'col1': 'value', 'col2': 'value2'}
    )
    encryption_cls.decrypt = mocker.Mock(return_value=controller._current_df)
    controller._encryption_cls = encryption_cls

    result = controller.decrypt(
        configs_path=str(tmp_path),
        config_file_name='decrypt.conf',
        config_encrypted=False,
        key_file_name='key.key',
        columns_mapping={'col1': 'col1new'}
    )

    encryption_cls.decrypt.assert_called_once_with({'col1new': 'value',
                                                    'col2': 'value2'})
    assert controller._decrypted is True
    assert result is controller._current_df
    assert controller._Controller__encryptor._columns == ['col1new', 'col2']
    controller._config_processor.read_config.assert_called_once_with(
        str(tmp_path),
        conf_file_name='decrypt.conf',
        config_encrypted=False,
        key_file_name='key.key'
    )


@pytest.mark.parametrize('save', [True, False])
def test_perform_pca(controller, mocker, save, tmp_path):
    mock_analysis = mocker.patch(
        'py4phi.controller.PrincipalComponentAnalysis.component_analysis'
    )
    mock_write = mocker.patch('py4phi.controller.PandasDatasetHandler.write')
    mock_prepare_location = mocker.patch('py4phi.controller.prepare_location')

    controller.perform_pca(save_reduced=save, save_folder=tmp_path)

    if save:
        mock_prepare_location.assert_called_once_with(
            location=str(tmp_path / DEFAULT_PCA_REDUCED_FOLDER_NAME)
        )
    mock_analysis.assert_called_once()
    assert not save or mock_write.call_count == 1


@pytest.mark.parametrize('save', [True, False])
def test_perform_feature_selection(controller, mocker, save, tmp_path):
    mock_analysis = mocker.patch(
        'py4phi.controller.FeatureSelection.correlation_analysis'
    )
    mock_write = mocker.patch('py4phi.controller.PandasDatasetHandler.write')
    mock_prepare_location = mocker.patch('py4phi.controller.prepare_location')
    controller.perform_feature_selection(
        target_feature='col1',
        drop_recommended=save,
        save_folder=tmp_path
    )

    if save:
        mock_prepare_location.assert_called_once_with(
            location=str(tmp_path / DEFAULT_FEATURE_SELECTION_FOLDER_NAME)
        )
    mock_analysis.assert_called_once()
    assert not save or mock_write.call_count == 1

"""Test ConfigProcessor class."""
import os

import pytest

from py4phi.config_processor import (
    DEFAULT_CONFIG_NAME,
    DEFAULT_SECRET_NAME,
    ConfigProcessor,
)


@pytest.fixture()
def processor():
    return ConfigProcessor()


def test_generate_key(processor, tmp_path):
    target_path = tmp_path / "test.config"
    processor._ConfigProcessor__generate_key(target_path)
    with open(target_path, 'rb') as file:
        contents = file.read()
        assert len(contents) == 32
        assert isinstance(contents, (bytes, bytearray))


def test_read_key(processor, tmp_path):
    target_path = tmp_path / "test.config"
    processor._ConfigProcessor__generate_key(target_path)
    key = processor._ConfigProcessor__read_key(target_path)
    assert len(key) == 32
    assert isinstance(key, (bytes, bytearray))


def test_read_key_dir(processor, tmp_path):
    with pytest.raises(IsADirectoryError):
        processor._ConfigProcessor__read_key(tmp_path)


def test_read_key_wrong_path(processor, tmp_path):
    with pytest.raises(FileNotFoundError):
        processor._ConfigProcessor__read_key(tmp_path / "wrong_path.txt")


def test_save_config(processor, tmp_path):
    columns_dict = {
        'col1': {'key': 'val'},
        'col2': {'aad': 'val'}
    }
    processor.save_config(columns_dict, tmp_path)
    assert os.path.exists(os.path.join(tmp_path, DEFAULT_CONFIG_NAME))
    assert os.path.exists(os.path.join(tmp_path, DEFAULT_SECRET_NAME))


def test_save_config_not_encrypted(processor, tmp_path):
    columns_dict = {
        'col1': {'key': 'val'},
        'col2': {'aad': 'val'}
    }
    processor.save_config(columns_dict, tmp_path, False)
    assert os.path.exists(os.path.join(tmp_path, DEFAULT_CONFIG_NAME))
    assert not os.path.exists(os.path.join(tmp_path, DEFAULT_SECRET_NAME))


@pytest.mark.parametrize('encrypt', [True, False])
def test_read_config(processor, tmp_path, encrypt):
    columns_dict = {
        'col1': {'key': 'val'},
        'col2': {'aad': 'val'}
    }
    processor.save_config(columns_dict, tmp_path, encrypt)
    res = processor.read_config(tmp_path, encrypt)
    assert res == columns_dict


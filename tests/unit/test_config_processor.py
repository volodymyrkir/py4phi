"""Test ConfigProcessor class."""

import pytest

from py4phi.config_processor import ConfigProcessor


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

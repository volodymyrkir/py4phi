"""Test pyspark encryptor logic."""
from secrets import token_hex

import pytest

from py4phi._encryption._model_encryptor import ModelEncryptor


@pytest.fixture()
def encryptor():
    return ModelEncryptor()


def test_encrypt_folder(tmp_path_factory, encryptor, mocker):
    mock_aes = mocker.MagicMock()
    mocker.patch('py4phi._encryption._model_encryptor.AES.new', mock_aes)
    mocker.patch('py4phi._encryption._model_encryptor.b64encode',
                 mocker.MagicMock(return_value=b'1'))
    mock_cipher = mock_aes.return_value
    mock_cipher.encrypt_and_digest.return_value = (b'encrypted_data', b'tag')
    base = tmp_path_factory.getbasetemp()
    res = encryptor.encrypt_folder(base)

    mock_aes.assert_called_once_with(mocker.ANY, mocker.ANY, nonce=mocker.ANY)
    mock_cipher.encrypt_and_digest.assert_called_once_with(mocker.ANY)
    assert res == {'model': {'key': mocker.ANY, 'aad': mocker.ANY}}


def test_decrypt_folder(tmp_path, mocker,encryptor):
    encrypted_dir = tmp_path / "encrypted"
    encrypted_dir.mkdir()
    encrypted_files = [
        encrypted_dir / f"file{i}.csv.encrypted" for i in range(3)
    ]
    for file_path in encrypted_files:
        with open(file_path, "wb") as f:
            f.write(b"encrypted_data")

    mock_aes_new = mocker.MagicMock()
    mocker.patch('py4phi._encryption._model_encryptor.AES.new', mock_aes_new)
    b644_mock = mocker.MagicMock(return_value=list(range(33)))
    mocker.patch('py4phi._encryption._model_encryptor.b64decode', b644_mock)
    unpad_mock = mocker.MagicMock(return_value=b'123')
    mocker.patch('py4phi._encryption._model_encryptor.unpad', unpad_mock)

    encryptor.decrypt_folder(encrypted_dir, token_hex(16), token_hex(16))

    assert all(file_path.with_suffix("").exists() and not file_path.exists()
               for file_path in encrypted_files)

    assert all(mock.call_count == len(encrypted_files)
               for mock in [mock_aes_new, unpad_mock, b644_mock])

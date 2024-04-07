"""Test pyspark encryptor logic."""
import shutil
from secrets import token_hex

import pytest

from py4phi._encryption._model_encryptor import ModelEncryptor


@pytest.fixture()
def encryptor():
    return ModelEncryptor()


@pytest.fixture
def test_folder(tmp_path):
    folder = tmp_path / "test_folder"
    folder.mkdir()
    file1 = folder / "file1.txt"
    file2 = folder / "file2.txt"
    file1.write_bytes(b"file1 content")
    file2.write_bytes(b"file2 content")
    yield folder
    shutil.rmtree(folder)


def test_encrypt_folder(test_folder, mocker, encryptor):
    mock_aes = mocker.MagicMock()
    mocker.patch("py4phi._encryption._model_encryptor.AES.new", mock_aes)
    mocker.patch("py4phi._encryption._model_encryptor.os.remove")
    mocker.patch('py4phi._encryption._model_encryptor.b64encode',
                 mocker.MagicMock(return_value=b'1'))
    mock_cipher = mock_aes.return_value
    mock_cipher.encrypt_and_digest.return_value = (b'encrypted_data', b'tag')

    result = encryptor.encrypt_folder(test_folder)

    assert "model" in result
    assert "key" in result["model"]
    assert "aad" in result["model"]
    assert len(result["model"]["key"]) == 32
    assert len(result["model"]["aad"]) == 32


def test_encrypt_folder_not_exists(test_folder, encryptor):
    fake_folder = test_folder / 'fake_folder'

    with pytest.raises(FileNotFoundError):
        encryptor.encrypt_folder(fake_folder)


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

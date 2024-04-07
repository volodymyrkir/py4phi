"""Test base encryptor logic."""
from unittest.mock import call

import pytest

from py4phi._encryption._encryptor import _BaseEncryptor


@pytest.fixture()
def encryptor(mocker):
    mocker.patch.object(_BaseEncryptor, '__abstractmethods__', set())
    return _BaseEncryptor(mocker.MagicMock(), ['col1', 'col2'])


def test_encrypt(encryptor, mocker):
    mock_encrypt_col = mocker.MagicMock(return_value='df')
    mocker.patch.object(_BaseEncryptor, '_encrypt_column', mock_encrypt_col)
    df, cols = encryptor.encrypt()
    assert mock_encrypt_col.call_count == len(encryptor._columns)
    assert df == 'df'
    assert cols == {
        'col1': {'aad': None, 'key': None},
        'col2': {'aad': None, 'key': None}
    }


@pytest.mark.parametrize('decryption_dict,columns', [
    ({'col2': {'key': 'val'}, 'col1': {'key': 'val'}}, ['col2']),
    ({'col2': {'key': 'val'}}, ['col2', 'col1'])
])
def test_decrypt(encryptor, mocker, decryption_dict, columns):
    mock_decrypt_col = mocker.MagicMock(return_value='df')
    mocker.patch.object(_BaseEncryptor, '_decrypt_column', mock_decrypt_col)
    mock_error = mocker.patch('py4phi._encryption._encryptor.logger.error')
    encryptor._columns = columns

    df = encryptor.decrypt(decryption_dict)
    assert mock_decrypt_col.call_count == 1
    assert df == 'df'
    expected_calls = [call(col, decryption_dict[col])
                      for col in columns if col in decryption_dict]
    mock_decrypt_col.assert_has_calls(expected_calls, any_order=True)
    assert mock_error.call_count == len(set(columns).difference(set(decryption_dict.keys())))


def test_get_and_save_salt(encryptor):
    for col in encryptor._columns:
        encryptor._get_and_save_salt(col)
    assert all([
        encryptor._columns[col]['key']
        and encryptor._columns[col]['aad']
        for col in encryptor._columns
    ])
    assert all([
        len(encryptor._columns[col]['key']) == 32
        and len(encryptor._columns[col]['aad']) == 32
        for col in encryptor._columns
    ])


def test_get_and_save_salt_wrong_col(encryptor):
    with pytest.raises(ValueError):
        encryptor._get_and_save_salt('non_existing_col')

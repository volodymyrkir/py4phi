"""Test pandas encryptor logic."""

import pandas as pd
import pytest

from py4phi._encryption._pandas_encryptor import _PandasEncryptor


@pytest.fixture()
def encryptor(mocker):
    df = mocker.MagicMock()
    df.columns = ['col1']
    encryptor = _PandasEncryptor(df, ['col1'])
    encryptor._columns['col1']['key'] = 'bfe4b06b65c952b46fa6941bdc5d10b9'
    encryptor._columns['col1']['aad'] = 'e20bab5c86ef1f9f42b6d854f3968958'
    return encryptor


@pytest.fixture(scope="module")
def encrypted_df():
    values = [
        '4gurXIbvH59CtthU85aJWK8VMbGm7xp9/V5cOst3PtZY5YPTBtXU5lTjPRsmgAG3Iev+2TI3r6PQcj5ca0Y+6Q==',
        '4gurXIbvH59CtthU85aJWMYj7uwyDcfmf1tRz'
        '+Aep3tTwr2eAMDB5ljoKgwmoh61PvGwhTIr7KTZJi9QJ2VtmJBuXPXTFkQA152wVcuj5qsOTOQmXx8A/J+X+5L6C1LT'
    ]
    return pd.DataFrame.from_dict({'col1': values})


@pytest.fixture(scope="module")
def decrypted_df():
    values = [
        'Commissioned Corps (excepted)',
        'HHS officers appointed by the President (exempt)'
    ]
    return pd.DataFrame.from_dict({'col1': values})


@pytest.mark.parametrize('column', [None, 123, 'col2'])
def test_encrypt_column_wrong(encryptor, column):
    with pytest.raises(ValueError):
        encryptor._encrypt_column(column)


def test_encrypt_column(encryptor, decrypted_df, encrypted_df, mocker):
    encryptor._df = decrypted_df.copy()
    mocker.patch.object(encryptor, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor._encrypt_column('col1')
    pd.testing.assert_frame_equal(actual_df, encrypted_df)


def test_decrypt_column(encryptor, decrypted_df, encrypted_df, mocker):
    encryptor._df = encrypted_df.copy()
    mocker.patch.object(encryptor, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor._decrypt_column('col1', encryptor._columns)
    pd.testing.assert_frame_equal(actual_df, decrypted_df)


@pytest.mark.parametrize('column', [None, 123, 'col2'])
def test_decrypt_column_wrong(encryptor, column, mocker):
    with pytest.raises(ValueError):
        encryptor._decrypt_column(column, mocker.MagicMock())

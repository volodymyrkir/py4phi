"""Test pandas encryptor logic."""
from itertools import product

import pandas as pd
import polars as pl
import pytest

from py4phi._encryption._pandas_encryptor import _PandasEncryptor
from py4phi._encryption._polars_encryptor import _PolarsEncryptor


@pytest.fixture()
def encryptor_pandas(mocker):
    df = mocker.MagicMock()
    df.columns = ['col1', 'col2']
    encryptor = _PandasEncryptor(df, ['col1'])
    encryptor._columns['col1']['key'] = 'bfe4b06b65c952b46fa6941bdc5d10b9'
    encryptor._columns['col1']['aad'] = 'e20bab5c86ef1f9f42b6d854f3968958'
    df['col2'] = None
    return encryptor


@pytest.fixture()
def encryptor_polars(mocker):
    df = mocker.MagicMock()
    df.columns = ['col1', 'col2']
    encryptor = _PolarsEncryptor(df, ['col1'])
    encryptor._columns['col1']['key'] = 'bfe4b06b65c952b46fa6941bdc5d10b9'
    encryptor._columns['col1']['aad'] = 'e20bab5c86ef1f9f42b6d854f3968958'
    return encryptor


@pytest.fixture()
def encrypted_df():
    values = [
        '4gurXIbvH59CtthU85aJWK8VMbGm7xp9/V5cOst3PtZY5YPTBtXU5lTjPRsmgAG3Iev+2TI3r6PQcj5ca0Y+6Q==',
        '4gurXIbvH59CtthU85aJWMYj7uwyDcfmf1tRz'
        '+Aep3tTwr2eAMDB5ljoKgwmoh61PvGwhTIr7KTZJi9QJ2VtmJBuXPXTFkQA152wVcuj5qsOTOQmXx8A/J+X+5L6C1LT'
    ]
    return pd.DataFrame.from_dict({'col1': values})


@pytest.fixture()
def decrypted_df():
    values = [
        'Commissioned Corps (excepted)',
        'HHS officers appointed by the President (exempt)'
    ]
    return pd.DataFrame.from_dict({'col1': values})


@pytest.mark.parametrize('column, specific_encryptor', [
    *product((None, 123, 'col5', 'col2'),
             ('encryptor_pandas', 'encryptor_polars'))
])
def test_encrypt_column_wrong(column, specific_encryptor, request):
    specific_encryptor = request.getfixturevalue(specific_encryptor)
    with pytest.raises(ValueError):
        specific_encryptor._encrypt_column(column)


def test_encrypt_column_pandas(encryptor_pandas, decrypted_df, encrypted_df, mocker):
    encryptor_pandas._df = decrypted_df
    mocker.patch.object(encryptor_pandas, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor_pandas._encrypt_column('col1')
    pd.testing.assert_frame_equal(actual_df, encrypted_df)


def test_encrypt_column_polars(encryptor_polars, decrypted_df, encrypted_df, mocker):
    df = pl.from_pandas(decrypted_df)
    encryptor_polars._df = df
    mocker.patch.object(encryptor_polars, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor_polars._encrypt_column('col1')
    assert pl.from_pandas(encrypted_df).equals(actual_df)


def test_decrypt_column(encryptor_pandas, decrypted_df, encrypted_df, mocker):
    encryptor_pandas._df = encrypted_df
    mocker.patch.object(encryptor_pandas, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor_pandas._decrypt_column('col1',
                                                 encryptor_pandas._columns['col1'])
    pd.testing.assert_frame_equal(actual_df, decrypted_df)


def test_decrypt_column_polars(encryptor_polars, decrypted_df, encrypted_df, mocker):
    encryptor_polars._df = pl.from_pandas(encrypted_df)
    mocker.patch.object(encryptor_polars, '_get_and_save_salt', mocker.MagicMock())
    actual_df = encryptor_polars._decrypt_column('col1',
                                                 encryptor_polars._columns['col1'])
    pl.from_pandas(decrypted_df).equals(actual_df)


@pytest.mark.parametrize('column, specific_encryptor', [
    *product((None, 123, 'col5'), ('encryptor_pandas', 'encryptor_polars'))
])
def test_decrypt_column_wrong(specific_encryptor, column, mocker, request):
    specific_encryptor = request.getfixturevalue(specific_encryptor)
    with pytest.raises(ValueError):
        specific_encryptor._decrypt_column(column, mocker.MagicMock())

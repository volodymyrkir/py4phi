"""Test pyspark encryptor logic."""

import pytest
from pyspark.sql import functions as f
from pyspark.sql import types as t

from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor


@pytest.fixture()
def encryptor(mocker):
    df = mocker.MagicMock()
    df.columns = ['col1', 'col2']
    return _PySparkEncryptor(df, ['col1'])


@pytest.fixture(scope="module")
def input_df(spark_session):
    return spark_session.createDataFrame([
        ['value1'],
        ['value2']
    ], schema=['col1'])


@pytest.fixture(scope="module")
def expected_df(spark_session):
    values = [
        ['Other'],
        ['Staff to be furloughed.']
    ]
    return spark_session.createDataFrame(values, schema=['col1'])


@pytest.fixture(scope="module")
def raw_df(spark_session):
    values = [
        ['tsxVkN8Aew5t9XRH/7o6n7bBGtXc9wAIKOKHEqHNnSeG'],
        ['TyJAnniSRB5YFhWeJfAyd13PP7nawpbjwbN0KKPXWlczOsUhtdAWoF2Ql2OXzNZZavYw']
    ]
    return spark_session.createDataFrame(values, schema=['col1'])


@pytest.mark.parametrize('column', [None, 123, 'col2'])
def test_encrypt_column_wrong(encryptor, column):
    with pytest.raises(ValueError):
        encryptor._encrypt_column(column)


def test_encrypt_column(encryptor, input_df):
    encryptor._df = input_df
    encrypted_df = encryptor._encrypt_column('col1')
    actual = encrypted_df.withColumn(
        'col1',
        f.aes_decrypt(
            f.unbase64(encrypted_df['col1']),
            key=f.lit(encryptor._columns['col1']['key']),
            aad=f.lit(encryptor._columns['col1']['aad'])
        ).cast(t.StringType())
    )

    assert actual.select('col1').collect() == input_df.select('col1').collect()


def test_decrypt_column(encryptor, raw_df, expected_df):
    encryptor._df = raw_df
    encryptor._columns['col1']['key'] = '465bb80db80fbacb77283840ba534f0c'
    encryptor._columns['col1']['aad'] = '113bc83374c8ea475cd2e48b07e36833'
    decrypted_df = encryptor._decrypt_column('col1', encryptor._columns['col1'])

    assert decrypted_df.select('col1').collect() == expected_df.select('col1').collect()


@pytest.mark.parametrize('column', [None, 123, 'col3'])
def test_decrypt_column_wrong(encryptor, column, mocker):
    with pytest.raises(ValueError):
        encryptor._decrypt_column(column, mocker.MagicMock())

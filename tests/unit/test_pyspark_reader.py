"""Contains tests for the BaseReader class."""
import pytest

from py4phi.readers.pyspark_reader import PySparkReader


@pytest.fixture(scope="module")
def reader():
    return PySparkReader()


def test_base_reader(mocker, reader):
    fake_df = mocker.MagicMock()
    reader.read_dataframe(fake_df)
    assert reader.df is fake_df



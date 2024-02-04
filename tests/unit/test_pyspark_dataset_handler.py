"""Contains tests for the BaseReader class."""
import pytest

from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler


@pytest.fixture(scope="module")
def reader():
    return PySparkDatasetHandler()


def test_base_reader(mocker, reader):
    fake_df = mocker.MagicMock()
    reader.read_dataframe(fake_df)
    assert reader.df is fake_df



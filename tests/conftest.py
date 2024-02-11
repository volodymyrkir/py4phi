import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def mock_logger(mocker):
    mock = mocker.MagicMock()
    mocker.patch('py4phi.logger_setup.logger', mock)
    return mock

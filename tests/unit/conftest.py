import pytest
from pyspark.sql import SparkSession


@pytest.fixture()
def spark_session():
    spark = SparkSession.builder.master("local").getOrCreate()
    yield spark
    spark.stop()

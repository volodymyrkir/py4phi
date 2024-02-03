"""Module for reading file or dataframe of pyspark."""
from pyspark.sql import SparkSession, DataFrame

from py4phi.readers.base_readers.base_reader import BaseReader

# TODO ADD LOGGING, ADD CUSTOM EXCEPTIONS, ADD FILE FORMATS SUPPORT, ADD TESTS, ADD PERSONAL ALLOWED TYPES MAPPING # noqa: E501


class PySparkReader(BaseReader):
    """Class for reading in file or PySpark dataframe."""

    def __init__(self):
        super().__init__()
        self._spark = SparkSession.builder.getOrCreate()

    def _to_pyspark(self, df: DataFrame) -> DataFrame:
        return df

    def _read_csv(self, path: str, **kwargs) -> DataFrame:
        """
        Read csv file with PySpark.

        Args:
        ----
        path (str): path to csv file.
        **kwargs (dict): Key-value pairs to pass to Spark csv reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.

        """
        return self._spark.read.csv(path, header=kwargs['header'])

    def _read_parquet(self, path: str, **kwargs) -> DataFrame:
        """
        Read parquet file with PySpark.

        Args:
        ----
        path (str): path to parquet file.
        **kwargs (dict): Key-value pairs to pass to Spark parquet reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.
        """
        return self._spark.read.parquet(path)



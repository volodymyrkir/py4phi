"""Module for reading file or dataframe of pyspark."""
import os
from typing import override

from pyspark.sql import SparkSession, DataFrame, types as t

from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler, PathOrStr
from py4phi.dataset_handlers.pyspark_write_utils import copy_merge_into

TMP_OUTPUT_DIR = "tmp-spark"
# TODO ADD LOGGING, ADD CUSTOM EXCEPTIONS, ADD FILE FORMATS SUPPORT, ADD TESTS, ADD PERSONAL ALLOWED TYPES MAPPING # noqa: E501


class PySparkDatasetHandler(BaseDatasetHandler):
    """Class for reading in file or PySpark dataframe."""

    def __init__(self):
        super().__init__()
        self._spark = SparkSession.builder.getOrCreate()

    def _to_pyspark(self, df: DataFrame) -> DataFrame:
        return df

    @override
    def _read_csv(self, path: PathOrStr, **kwargs) -> DataFrame:
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
        return self._spark.read.csv(path, header=kwargs['header'], multiLine=True)

    @override
    def _read_parquet(self, path: PathOrStr, **kwargs) -> DataFrame:
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

    @override
    def _write_csv(self, df: DataFrame, name: str, path: PathOrStr, **kwargs) -> None:
        """
        Write csv file with PySpark.

        Args:
        ----
        df (DataFrame): PySpark Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        suffix = '.csv' if not str(path).endswith('.csv') else ''

        headers = self._spark.createDataFrame(
            data=[[f.name for f in df.schema.fields]],
            schema=t.StructType(
                [t.StructField(f.name, t.StringType(), False) for f in df.schema.fields]
            ),
        )
        headers.write.csv(TMP_OUTPUT_DIR, mode='overwrite')
        copy_merge_into(
            self._spark,
            TMP_OUTPUT_DIR,
            os.path.join(path, name) + suffix,
            delete_source=True,
        )

        df.write.csv(TMP_OUTPUT_DIR, **kwargs)

        copy_merge_into(
            self._spark,
            TMP_OUTPUT_DIR,
            os.path.join(path, name) + suffix,
            delete_source=True,
        )

    @override
    def _write_parquet(
            self,
            df: DataFrame,
            name: str,
            path: PathOrStr,
            **kwargs
    ) -> None:
        """
        Write parquet file.

        Args:
        ----
        df (DataFrame): Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save parquet file.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        suffix = '.parquet' if not str(path).endswith('.parquet') else ''
        df.write.parquet(TMP_OUTPUT_DIR, **kwargs)

        copy_merge_into(
            self._spark,
            TMP_OUTPUT_DIR,
            os.path.join(path, name) + suffix,
            delete_source=True,
        )

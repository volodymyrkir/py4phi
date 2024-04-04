"""Module for reading file or dataframe by PySpark."""
import os
from importlib.resources import files

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, types as t

import py4phi
from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler, PathOrStr
from py4phi.dataset_handlers.pyspark_write_utils import copy_merge_into
from py4phi.consts import TMP_OUTPUT_DIR


class PySparkDatasetHandler(BaseDatasetHandler):
    """Class for reading in file or PySpark dataframe."""

    def __init__(self):
        super().__init__()
        log4j_props_path = files(py4phi) / "log4j.properties"
        self._spark = (
            SparkSession.builder.appName('py4phi')
            .config("spark.driver.extraJavaOptions",
                    f"-Dlog4j.configuration=file:{log4j_props_path}")
            .getOrCreate()
        )

    @staticmethod
    def print_df(df: DataFrame) -> None:
        """
        Print PySpark dataframe.

        Args:
        ----
        df (DataFrame): PySpark dataframe.

        Returns: None.

        """
        if not isinstance(df, DataFrame):
            raise ValueError('Non-PySpark DataFrame passed to PySparkDatasetHandler')
        df.show()

    def _read_csv(self, path: PathOrStr, **kwargs) -> DataFrame:
        """
        Read csv file with PySpark.

        Args:
        ----
        path (str): path to csv file.
        **kwargs (dict): Key-value pairs to pass to Spark csv reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.

        """
        return self._spark.read.csv(path, multiLine=True, **kwargs)

    def _read_parquet(self, path: PathOrStr, **kwargs) -> DataFrame:
        """
        Read parquet file with PySpark.

        Args:
        ----
        path (str): path to parquet file.
        **kwargs (dict): Key-value pairs to pass to Spark parquet reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.
        """
        return self._spark.read.parquet(path, **kwargs)

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

    def to_pandas(self) -> pd.DataFrame:
        """
        Cast current dataframe to pandas dataframe.

        Returns: (pd.Dataframe) Pandas dataframe.

        """
        return self._df.toPandas()

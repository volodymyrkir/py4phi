"""Module for reading file or dataframe by Polars."""
import os

import pandas as pd
import polars as pl

from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler, PathOrStr


class PolarsDatasetHandler(BaseDatasetHandler):
    """Class for reading in file or Polars dataframe."""

    def to_pandas(self) -> pd.DataFrame:
        """
        Cast current dataframe to pandas dataframe.

        Returns: (pd.Dataframe) Pandas dataframe.

        """
        return self._df.to_pandas()

    @staticmethod
    def print_df(df: pl.DataFrame) -> None:
        """
        Print PySpark dataframe.

        Args:
        ----
        df (DataFrame): Polars dataframe.

        Returns: None.

        """
        if not isinstance(df, pl.DataFrame):
            raise ValueError('Non-Polars DataFrame passed to PolarsDatasetHandler')
        with pl.Config(tbl_cols=df.width, fmt_str_lengths=1000, tbl_width_chars=1000):
            print(df.head(30))

    def _read_csv(self, path: PathOrStr, **kwargs) -> pl.DataFrame:
        """
        Read csv file with Polars.

        Args:
        ----
        path (str): path to csv file.
        **kwargs (dict): Key-value pairs to pass to Polars csv reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.

        """
        return pl.read_csv(path, **kwargs)

    def _read_parquet(self, path: PathOrStr, **kwargs) -> pl.DataFrame:
        """
        Read parquet file with Polars.

        Args:
        ----
        path (str): path to parquet file.
        **kwargs (dict): Key-value pairs to pass to Polars parquet reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.
        """
        return pl.read_parquet(
            path,
            **kwargs
        )

    def _write_csv(
            self,
            df: pl.DataFrame,
            name: str,
            path: PathOrStr,
            **kwargs
    ) -> None:
        """
        Write csv file with Polars.

        Args:
        ----
        df (DataFrame): Polars Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        suffix = '.csv' if not str(name).endswith('.csv') else ''

        df.write_csv(os.path.join(path, name + suffix), **kwargs)

    def _write_parquet(
            self,
            df: pl.DataFrame,
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

        df.write_parquet(os.path.join(path, name + suffix), **kwargs)

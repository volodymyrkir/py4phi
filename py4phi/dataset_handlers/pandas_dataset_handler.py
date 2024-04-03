"""Module for reading file or dataframe by Pandas."""
import os

import pandas as pd

from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler, PathOrStr


class PandasDatasetHandler(BaseDatasetHandler):
    """Class for reading in file or Pandas dataframe."""

    def to_pandas(self) -> pd.DataFrame:
        """
        Cast current dataframe to pandas dataframe.

        Returns: (pd.Dataframe) Pandas dataframe.

        """
        return self._df

    @staticmethod
    def print_df(df: pd.DataFrame) -> None:
        """
        Print PySpark dataframe.

        Args:
        ----
        df (DataFrame): PySpark dataframe.

        Returns: None.

        """
        if not isinstance(df, pd.DataFrame):
            raise ValueError('Non-Pandas DataFrame passed to PandasDatasetHandler')
        pd.set_option('display.max_columns', None)
        print(df.head(30))

    def _read_csv(self, path: PathOrStr, **kwargs) -> pd.DataFrame:
        """
        Read csv file with Pandas.

        Args:
        ----
        path (str): path to csv file.
        **kwargs (dict): Key-value pairs to pass to pandas csv reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.

        """
        return pd.read_csv(path, engine='pyarrow', dtype_backend='pyarrow', **kwargs)

    def _read_parquet(self, path: PathOrStr, **kwargs) -> pd.DataFrame:
        """
        Read parquet file with Pandas.

        Args:
        ----
        path (str): path to parquet file.
        **kwargs (dict): Key-value pairs to pass to Pandas parquet reading function.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the handler object.
        """
        return pd.read_parquet(
            path,
            engine='pyarrow',
            dtype_backend='pyarrow',
            **kwargs
        )

    def _write_csv(
            self,
            df: pd.DataFrame,
            name: str,
            path: PathOrStr,
            **kwargs
    ) -> None:
        """
        Write csv file with Pandas.

        Args:
        ----
        df (DataFrame): Pandas Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        suffix = '.csv' if not str(name).endswith('.csv') else ''

        df.to_csv(os.path.join(path, name + suffix), **kwargs)

    def _write_parquet(
            self,
            df: pd.DataFrame,
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

        df.to_parquet(os.path.join(path, name + suffix), **kwargs)

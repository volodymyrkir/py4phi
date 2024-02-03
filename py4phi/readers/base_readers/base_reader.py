"""Module that provides base class logic related to reading data."""
from abc import ABC, abstractmethod
from typing import Any


class BaseReader(ABC):
    """
    Abstract reader class.

    Defines basic reading functionality
     to be implemented and extended by particular readers.
    """

    def __init__(self):
        self._df = None

    def _get_reading_method(self, file_type: str) -> callable:
        method = {'CSV': self._read_csv, 'PARQUET': self._read_parquet}.get(file_type)
        if not method:
            raise NotImplementedError(f'No reading method for file type {file_type}')
        return method

    @property
    def df(self):
        """
        Return dataframe of the reader.

        Returns: dataframe of the reader.

        """
        return self._df

    def read_dataframe(self, df) -> None:  # TODO multiple engines
        """
        Assign given dataframe object to the class field.

        Args:
        ----
        df (Any): Dataframe object.

        Returns: None

        """
        self._df = self._to_pyspark(df)

    def read_file(self, path: str, file_type: str, **kwargs) -> None:
        """
        Read file by given path and type.

        Args:
        ----
        path (str): Path to the file to be read.

        file_type (str, optional): Type of the file to be read. Defaults to 'csv'.

        **kwargs (dict): Additional key-value pairs to be supplied to the reader.

        Returns: None

        """
        reading_method = self._get_reading_method(file_type=file_type.upper())
        self._df = reading_method(path, **kwargs)

    @abstractmethod
    def _read_csv(self, path: str, *args, **kwargs) -> Any:
        """
        Read csv file.

        Args:
        ----
        path: path to csv file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.

        """
        pass

    @abstractmethod
    def _read_parquet(self, path: str, *args, **kwargs) -> Any:
        """
        Read parquet file.

        Args:
        ----
        path: path to csv file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.

        """
        pass

    @abstractmethod
    def _to_pyspark(self, df):
        """
        Convert the dataframe to PySpark.

        Args:
        ----
        df: DataFrame to be converted to PySpark.

        Returns: Spark DataFrame

        """
        pass


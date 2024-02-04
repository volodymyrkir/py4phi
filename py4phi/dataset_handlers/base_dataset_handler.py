"""Module that provides base class logic related to reading data."""
from abc import ABC, abstractmethod
from typing import Any


class BaseDatasetHandler(ABC):
    """
    Abstract reader class.

    Defines basic reading functionality
     to be implemented and extended by particular dataset_handlers.
    """

    def __init__(self):
        self._df = None
        self._writing_method = None

    def _get_interaction_methods(self, file_type: str) -> tuple[callable, callable]:
        methods = {
            'CSV': (self._read_csv, self._write_csv),
            'PARQUET': (self._read_parquet, self._write_parquet)
        }.get(file_type)
        if not methods:
            raise NotImplementedError(f'No reading method for file type {file_type}')
        return methods

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
        reading_method, writing_method = self._get_interaction_methods(
            file_type=file_type.upper()
        )
        self._df = reading_method(path, **kwargs)
        self._writing_method = writing_method

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

    def write(self, df: Any, name: str, path: str, **kwargs):
        """
        Write file.

        Args:
        ----
        df (Any): Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        writing_method = self._writing_method or self._write_csv
        writing_method(df, name, path, **kwargs)

    @abstractmethod
    def _write_csv(self, df: Any, name: str, path: str, *args, **kwargs) -> None:
        """
        Write csv file.

        Args:
        ----
        df (Any): Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        pass

    @abstractmethod
    def _write_parquet(self, df: Any, name: str,  path: str, *args, **kwargs) -> None:
        """
        Write parquet file.

        Args:
        ----
        df (Any): Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save parquet file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns: None.

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


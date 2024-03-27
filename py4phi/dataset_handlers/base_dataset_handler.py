"""Module that provides base class logic related to reading data."""
from abc import ABC, abstractmethod
from typing import Callable, TypeAlias, Any

import pandas as pd

from py4phi.logger_setup import logger
from py4phi.utils import DataFrame, PathOrStr

ReadingFunction: TypeAlias = Callable[[PathOrStr, Any, Any], Any]
WritingFunction: TypeAlias = Callable[[Any, str, PathOrStr, Any, Any], None]


class BaseDatasetHandler(ABC):
    """
    Abstract reader class.

    Defines basic reading functionality
     to be implemented and extended by particular dataset_handlers.
    """

    def __init__(self):
        self._df: DataFrame = None
        self._writing_method = None

    def _get_interaction_methods(
            self,
            file_type: str | None,
    ) -> tuple[ReadingFunction, WritingFunction]:
        """
        Get interaction methods for particular file type.

        Args:
        ----
            file_type(str, optional): File type in str format.

        Returns: A pair of reading function and writing function.

        """
        if file_type:
            methods = {
                'CSV': (self._read_csv, self._write_csv),
                'PARQUET': (self._read_parquet, self._write_parquet)
            }.get(file_type.upper())
            if not methods:
                raise NotImplementedError(f'No reading/writing method '
                                          f'for file type {file_type}')
            logger.debug(f'Reading method for file type - {methods[0].__name__},'
                         f' writing method - {methods[1].__name__}.')
            return methods

    @property
    def df(self):
        """
        Return dataframe of the reader.

        Returns: dataframe of the handler.

        """
        return self._df

    def read_dataframe(self, df: DataFrame) -> None:
        """
        Assign given dataframe object to the class field.

        Args:
        ----
        df (DataFrame): Dataframe object.

        Returns: None

        """
        self._df = df

    def read_file(self, path: PathOrStr, file_type: str, **kwargs) -> None:
        """
        Read file by given path and type.

        Args:
        ----
        path (PathOrStr): Path to the file to be read.

        file_type (str, optional): Type of the file to be read. Defaults to 'csv'.

        **kwargs (dict): Additional key-value pairs to be supplied to the reader.

        Returns: None

        """
        reading_method, writing_method = self._get_interaction_methods(
            file_type=file_type
        )
        logger.debug(f'Reading {path} file using {kwargs} keyword args.')
        self._df = reading_method(path, **kwargs)
        self._writing_method = writing_method

    @abstractmethod
    def _read_csv(self, path: PathOrStr, *args, **kwargs) -> DataFrame:
        """
        Read csv file.

        Args:
        ----
        path (PathOrStr): path to csv file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.

        """
        pass

    @abstractmethod
    def _read_parquet(self, path: PathOrStr, *args, **kwargs) -> DataFrame:
        """
        Read parquet file.

        Args:
        ----
        path (PathOrStr): path to csv file.
        args: Positional arguments supplied to the function.
        kwargs: Additional key-value arguments.

        Returns:
        -------
            None: Assigns dataframe to self._df property of the reader object.

        """
        pass

    def write(
            self,
            df: Any,
            name: str,
            path: PathOrStr,
            save_format: str | None = None,
            **kwargs
    ) -> None:
        """
        Write file.

        Args:
        ----
        df (Any): Dataframe to be saved.
        name (str): Name of the resulting file.
        path (str): path to save csv file.
        save_format (str, optional): Writing format. Defaults to None.
        kwargs: Additional key-value arguments.

        Returns: None.

        """
        writing_method = (
                [*self._get_interaction_methods(save_format)][1]
                if save_format else self._writing_method or self._write_csv
        )
        logger.debug(f'Writing dataframe to {path} '
                     f'with name {name}, '
                     f'using {kwargs} keyword args.')
        writing_method(df, name, path, **kwargs)

    @abstractmethod
    def _write_csv(
            self,
            df: DataFrame,
            name: str,
            path: PathOrStr,
            *args,
            **kwargs
    ) -> None:
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
    def _write_parquet(
            self,
            df: DataFrame,
            name: str,
            path: PathOrStr,
            *args,
            **kwargs
    ) -> None:
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

    @staticmethod
    @abstractmethod
    def print_df(df: DataFrame) -> None:
        """
        Print dataframe.

        Args:
        ----
        df (DataFrame): DataFrame.

        Returns: None.

        """
        pass

    @abstractmethod
    def to_pandas(self) -> pd.DataFrame:
        """
        Cast current dataframe to pandas dataframe.

        Returns: (pd.Dataframe) Pandas dataframe.

        """
        pass

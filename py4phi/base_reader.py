"""Module that provides base class logic related to reading data."""
from abc import ABC, abstractmethod
from typing import Optional


class BaseReader(ABC):
    """
    Abstract reader class.

    Defines basic reading functionality
     to be implemented and extended by particular readers.
    """

    supported_file_types = {}

    def __init__(self):
        """Define dataframe to be read as None."""
        self._df = None

    def read_dataframe(self, df) -> None:
        """
        Assign given dataframe object to the class field.

        Args:
        ----
        df (Any): Dataframe object.

        Returns: None

        """
        self._df = df

    @abstractmethod
    def read_file(self, path: Optional[str], file_type: str = 'csv', **kwargs) -> None:
        """
        Read file by given path and type.

        Args:
        ----
        path (str): Path to the file to be read.

        file_type (str, optional): Type of the file to be read. Defaults to 'csv'.

        **kwargs (dict): Additional key-value pairs to be supplied to the reader.

        Returns: None

        """
        pass

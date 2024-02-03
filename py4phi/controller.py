"""Module containing main logic and entrypoint for library."""
import os
from typing import Optional

from pyspark.sql import DataFrame

from py4phi.config_processor import ConfigProcessor
from py4phi.readers.base_readers.base_reader import BaseReader
from py4phi.readers.pyspark_reader import PySparkReader
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor

readers_mapping = {
    DataFrame: PySparkReader,

}


class Controller:
    """Class to manipulate df."""

    def __init__(self, reader: BaseReader):
        self._reader = reader
        self.__encryptor = None
        self.__columns_data = None
        self._current_df = self._reader.df
        self._config_processor = ConfigProcessor()

    def print_current_df(self) -> None:
        """
        Print dataframe of current state.

        Returns: None

        """
        self._current_df.show()

    def encrypt_and_save(self, columns_to_encrypt: list[str]):
        """
        Encrypt specified columns in dataset.

        Args:
        ----
            columns_to_encrypt (list[str]): List of columns to be encrypted.

        Returns: Encrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df, columns_to_encrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        self._current_df, self.__columns_data = self.__encryptor.encrypt()
        self._config_processor.save_config(self.__columns_data)

    def decrypt(
            self,
            columns_to_decrypt: list[str],
            configs_path: str = os.getcwd()
    ) -> None:
        """
        Decrypt specified columns in dataset.

        Args:
        ----
        columns_to_decrypt (list[str]): List of columns to be decrypted.
        configs_path (str, optional): Path to the directory,
                                        containing the decryption configs.
                                        Defaults to os.getcwd().

        Returns: Decrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df,
                              columns_to_decrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        decryption_dict = self._config_processor.read_config(configs_path)
        self._current_df = self.__encryptor.decrypt(decryption_dict)


def init_from_path(
        path: str,
        file_type: str,
        header: Optional[bool] = False
) -> Controller:
    """
    Initialize controller object via given path and file type to interact with data.

    Args:
    ----
    path (str): Path to file to be read.
    file_type (str): File type for given file. Defaults to CSV.
    header (bool): Boolean flag indicating whether to use
        the first line of the file as a header. Defaults to True.

    Returns: Controller object.
    """
    reader = PySparkReader()

    reader.read_file(path=path, file_type=file_type, header=header)

    return Controller(reader)


def init_from_dataframe(df):
    """
    Initialize controller object via given dataframe object.

    Args:
    ----
        df: DataFrame to be read. Read docs to see available libraries.

    Returns: Controller object.

    """
    if type(df) not in readers_mapping.keys():
        raise ValueError(f'Unsupported object of type {type(df)}.')
    reader = readers_mapping[type(df)]()
    reader.read_dataframe(df)

    return Controller(reader)

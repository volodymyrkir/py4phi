"""Module containing main logic and entrypoint for library."""
import os
from typing import Optional

from pyspark.sql import DataFrame

from py4phi.config_processor import ConfigProcessor
from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor

DEFAULT_PY4PHI_OUTPUT_PATH = os.path.join(os.getcwd(), 'py4phi_outputs')

readers_mapping = {
    DataFrame: PySparkDatasetHandler,

}


class Controller:
    """Class to manipulate df."""

    def __init__(self, dataset_handler: BaseDatasetHandler):
        self._dataset_handler = dataset_handler
        self.__encryptor = None
        self.__columns_data = None
        self._current_df = self._dataset_handler.df
        self._config_processor = ConfigProcessor()

    def print_current_df(self) -> None:
        """
        Print dataframe of current state.

        Returns: None

        """
        self._current_df.show()

    def encrypt_and_save(
            self,
            columns_to_encrypt: list[str],
            output_name: str = 'output_dataset',
            save_location: str = DEFAULT_PY4PHI_OUTPUT_PATH,
            encrypt_config: bool = True,
            **kwargs
    ):
        """
        Encrypt specified columns in dataset.

        Args:
        ----
        columns_to_encrypt (list[str]): List of columns to be encrypted.
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config. Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        Returns: Encrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df, columns_to_encrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        self._current_df, self.__columns_data = self.__encryptor.encrypt()
        os.makedirs(save_location, exist_ok=True)
        self._config_processor.save_config(
            self.__columns_data,
            path=save_location,
            encrypt_config=encrypt_config
        )
        self._dataset_handler.write(
            self._current_df,
            output_name,
            DEFAULT_PY4PHI_OUTPUT_PATH,
            **kwargs
        )

    def decrypt(
            self,
            columns_to_decrypt: list[str],
            configs_path: str = DEFAULT_PY4PHI_OUTPUT_PATH,
            config_encrypted: bool = True,
    ) -> None:
        """
        Decrypt specified columns in dataset.

        Args:
        ----
        columns_to_decrypt (list[str]): List of columns to be decrypted.
        configs_path (str, optional): Path to the directory,
                                        containing the decryption configs.
                                        Defaults to current working directory.
        config_encrypted (bool): Whether config is encrypted. Defaults to True.

        Returns: Decrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df,
                              columns_to_decrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        decryption_dict = self._config_processor.read_config(
            configs_path,
            config_encrypted=config_encrypted
        )
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
    reader = PySparkDatasetHandler()

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

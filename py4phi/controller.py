"""Module containing main logic and entrypoint for library."""
import shutil
import os
from typing import Optional
from logging import INFO

from pyspark.sql import DataFrame

from py4phi.config_processor import ConfigProcessor
from py4phi.dataset_handlers.base_dataset_handler import BaseDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor

from py4phi.logger_setup import logger
from py4phi.consts import (
    DEFAULT_CONFIG_NAME, DEFAULT_SECRET_NAME, DEFAULT_PY4PHI_OUTPUT_PATH
)

handlers_mapping = {
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
        logger.info('Printing active dataframe.')
        self._current_df.show()

    def encrypt_and_save(
            self,
            columns_to_encrypt: list[str],
            output_name: str = 'output_dataset',
            save_location: str = DEFAULT_PY4PHI_OUTPUT_PATH,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            encrypt_config: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME,
            **kwargs
    ):
        """
        Encrypt specified columns in dataset.

        Args:
        ----
        columns_to_encrypt (list[str]): List of columns to be encrypted.
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config.
                                            Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        config_file_name (str, optional): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str, optional): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        Returns: Encrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df, columns_to_encrypt)
            if not self.__encryptor
            else self.__encryptor
        )

        logger.info(f'Kicking off encryption on current dataframe '
                    f'for columns: {columns_to_encrypt}.')
        self._current_df, self.__columns_data = self.__encryptor.encrypt()

        shutil.rmtree(save_location, ignore_errors=True)
        os.makedirs(save_location, exist_ok=True)
        logger.debug(f'Successfully prepared save location: {save_location}.')

        self._config_processor.save_config(
            self.__columns_data,
            path=save_location,
            conf_file_name=DEFAULT_CONFIG_NAME,
            encrypt_config=encrypt_config,
            key_file_name=DEFAULT_SECRET_NAME
        )
        logger.debug(f'Saved config to: {save_location}.')
        self._dataset_handler.write(
            self._current_df,
            output_name,
            DEFAULT_PY4PHI_OUTPUT_PATH,
            **kwargs
        )
        logger.info(f'Saved outputs to: {save_location}.')

    def decrypt(
            self,
            columns_to_decrypt: list[str],
            configs_path: str = DEFAULT_PY4PHI_OUTPUT_PATH,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            config_encrypted: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME
    ) -> None:
        """
        Decrypt specified columns in dataset.

        Args:
        ----
        columns_to_decrypt (list[str]): List of columns to be decrypted.
        configs_path (str, optional): Path to the directory,
                                        containing the decryption configs.
                                        Defaults to current working directory.
        config_encrypted (bool): Whether config is encrypted.
                                    Defaults to True.
        config_file_name (str): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.

        Returns: Decrypted dataframe.

        """
        self.__encryptor = (
            _PySparkEncryptor(self._current_df,
                              columns_to_decrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        logger.info(f'Kicking off decryption on current dataframe on columns: '
                    f'{columns_to_decrypt}. '
                    f'Config path is {configs_path}. '
                    f'{'Config is encrypted' if config_encrypted else ''}')
        decryption_dict = self._config_processor.read_config(
            configs_path,
            conf_file_name=config_file_name,
            config_encrypted=config_encrypted,
            key_file_name=key_file_name
        )

        self._current_df = self.__encryptor.decrypt(decryption_dict)
        logger.info('Successfully decrypted current df.')


def init_from_path(
        path: str,
        file_type: str,
        header: Optional[bool] = False,
        log_level: str | int = INFO
) -> Controller:
    """
    Initialize controller object via given path and file type to interact with data.

    Args:
    ----
    path (str): Path to file to be read.
    file_type (str): File type for given file.
                        Defaults to CSV.
    header (bool): Boolean flag indicating whether to use
        the first line of the file as a header. Defaults to True.
    log_level (str|int): Logging level. Set to DEBUG for debugging.
                     Defaults to INFO.

    Returns: Controller object.
    """
    logger.setLevel(log_level)
    logger.debug("Initializing Dataset Handler")
    reader = PySparkDatasetHandler()
    logger.info(f"Reading dataframe using {type(reader)} "
                f"from file: {path}, of type {file_type}.")
    reader.read_file(path=path, file_type=file_type, header=header)
    return Controller(reader)


def init_from_dataframe(df, log_level: str | int = INFO) -> Controller:
    """
    Initialize controller object via given dataframe object.

    Args:
    ----
    df: DataFrame to be read. Read docs to see available libraries.
    log_level (str|int): Logging level. Set to DEBUG for debugging.
                         Defaults to INFO.

    Returns: Controller object.

    """
    logger.setLevel(log_level)
    if type(df) not in handlers_mapping.keys():
        raise ValueError(f'Unsupported object of type {type(df)}.')
    handler = handlers_mapping[type(df)]()
    logger.debug(f"Using {type(handler)} dataset handler")
    logger.info(f"Reading dataframe of type {type(df)}.")
    handler.read_dataframe(df)

    return Controller(handler)

"""Module containing main logic and entrypoint for library."""
import shutil
import os
from logging import INFO
from typing import Type

import pandas as pd
from pyspark.sql import DataFrame

from py4phi.config_processor import ConfigProcessor
from py4phi.dataset_handlers.base_dataset_handler import (
    BaseDatasetHandler, DataFrame as BaseDF
)
from py4phi.dataset_handlers.pandas_dataset_handler import PandasDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor
from py4phi._encryption._pandas_encryptor import _PandasEncryptor, _BaseEncryptor

from py4phi.logger_setup import logger
from py4phi.consts import (
    DEFAULT_CONFIG_NAME, DEFAULT_SECRET_NAME,
    DEFAULT_PY4PHI_ENCRYPTED_PATH, DEFAULT_PY4PHI_DECRYPTED_PATH
)


class Controller:
    """Class to interact with py4phi components."""

    HANDLERS_TYPE_MAPPING = {
        DataFrame: PySparkDatasetHandler,
        pd.DataFrame: PandasDatasetHandler,
    }

    ENGINE_NAME_MAPPING = {
        'pyspark': PySparkDatasetHandler,
        'pandas': PandasDatasetHandler,
    }

    ENCRYPTION_MAPPING: dict[Type[BaseDatasetHandler], Type[_BaseEncryptor]] = {
        PySparkDatasetHandler: _PySparkEncryptor,
        PandasDatasetHandler: _PandasEncryptor
    }

    def __init__(self, dataset_handler: BaseDatasetHandler):
        self._dataset_handler = dataset_handler
        self._encryption_cls = self.ENCRYPTION_MAPPING[type(dataset_handler)]
        self.__encryptor = None
        self._encrypted: bool = False
        self._decrypted: bool = False
        self.__columns_data = None
        self._current_df: BaseDF = self._dataset_handler.df
        self._config_processor = ConfigProcessor()

    def print_current_df(self) -> None:
        """
        Print dataframe of current state.

        Returns: None

        """
        logger.info('Printing active dataframe.')
        self._dataset_handler.print_df(self._current_df)

    def encrypt(
            self,
            columns_to_encrypt: list[str],
    ) -> BaseDF:
        """
        Encrypt specified columns in dataset.

        Args:
        ----
        columns_to_encrypt (list[str]): List of columns to be encrypted.

        Returns: None.

        """
        self.__encryptor = (
            self._encryption_cls(self._current_df, columns_to_encrypt)
            if not self.__encryptor
            else self.__encryptor
        )

        logger.info(f'Kicking off encryption on current dataframe '
                    f'for columns: {columns_to_encrypt}.')
        self._current_df, self.__columns_data = self.__encryptor.encrypt()
        self._encrypted = True
        logger.info('Successfully encrypted dataframe.')
        return self._current_df

    def save_encrypted(
            self,
            output_name: str = 'output_dataset',
            save_location: str = DEFAULT_PY4PHI_ENCRYPTED_PATH,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            encrypt_config: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME,
            **kwargs
    ) -> None:
        """
        Save dataframe using provided location and parameters.

        Args:
        ----
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config.
                                            Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        config_file_name (str, optional): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str, optional): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        Returns: None.

        """
        if not self._encrypted:
            logger.warn('No encryption action taken! '
                        'Perhaps you forgot to encrypt your dataframe, aborting.')
            exit(1)

        shutil.rmtree(save_location, ignore_errors=True)
        os.makedirs(save_location, exist_ok=True)
        logger.debug(f'Successfully prepared save location: {save_location}.')

        self._config_processor.save_config(
            self.__columns_data,
            path=save_location,
            conf_file_name=config_file_name,
            encrypt_config=encrypt_config,
            key_file_name=key_file_name
        )
        logger.debug(f'Saved config to: {save_location}.')
        self._dataset_handler.write(
            self._current_df,
            output_name,
            save_location,
            **kwargs
        )
        logger.info(f'Saved outputs to: {save_location}.')

    def save_decrypted(
            self,
            output_name: str = 'output_dataset',
            save_location: str = DEFAULT_PY4PHI_DECRYPTED_PATH,
            **kwargs
    ) -> None:
        """
        Save decrypted dataframe. Can be used without.

        Args:
        ----
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config.
                                            Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        config_file_name (str, optional): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str, optional): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        Returns: None.

        """
        if not self._decrypted:
            logger.warn('No decryption action taken! '
                        'Perhaps you forgot to decrypt your dataframe. '
                        'Saving non-decrypted data.')
        shutil.rmtree(save_location, ignore_errors=True)
        os.makedirs(save_location, exist_ok=True)
        logger.debug(f'Successfully prepared save location: {save_location}.')

        self._dataset_handler.write(
            self._current_df,
            output_name,
            save_location,
            **kwargs
        )
        logger.info(f'Saved outputs to: {save_location}.')

    def decrypt(
            self,
            columns_to_decrypt: list[str],
            configs_path: str = DEFAULT_PY4PHI_ENCRYPTED_PATH,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            config_encrypted: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME
    ) -> BaseDF:
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
            self._encryption_cls(self._current_df, columns_to_decrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        if self.__columns_data:
            decryption_dict = self.__columns_data
            logger.debug("Decrypting previously encrypted dataframe, "
                         "ignoring provided configs paths...")
        else:
            logger.info(f'Kicking off decryption on current dataframe on columns: '
                        f'{columns_to_decrypt}. '
                        f'Config path is {configs_path}. '
                        f'{'Config is encrypted' if config_encrypted else ''}')
            try:
                decryption_dict = self._config_processor.read_config(
                    configs_path,
                    conf_file_name=config_file_name,
                    config_encrypted=config_encrypted,
                    key_file_name=key_file_name
                )
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Decryption config files not found under {configs_path}. "
                )

        self._current_df = self.__encryptor.decrypt(decryption_dict)
        self._decrypted = True
        logger.info('Successfully decrypted current df.')
        return self._decrypted


def from_path(
        path: str,
        file_type: str,
        engine: str = 'pyspark',
        log_level: str | int = INFO,
        **kwargs
) -> Controller:
    """
    Initialize controller object via given path and file type to interact with data.

    Args:
    ----
    path (str): Path to file to be read.
    file_type (str): File type for given file.
                        Defaults to CSV.
    engine (str, optional): Engine to use. Can be 'pandas', 'pyspark', TBD.
                                Defaults to 'pyspark'.
    header (bool): Boolean flag indicating whether to use
        the first line of the file as a header. Defaults to True.
    log_level (str|int): Logging level. Set to DEBUG for debugging.
                     Defaults to INFO.
    kwargs (dict): Optional keyword arguments to pass to reading method.

    Returns: Controller object.
    """
    logger.setLevel(log_level)

    logger.debug("Initializing Dataset Handler")
    reader_cls = Controller.ENGINE_NAME_MAPPING.get(engine)
    if not reader_cls:
        raise KeyError(f"No such engine: {engine},"
                       f" could be one of {tuple(Controller.ENGINE_NAME_MAPPING)}")
    reader = reader_cls()
    logger.info(f"Reading dataframe using {type(reader)} "
                f"from file: {path}, of type {file_type}.")

    reader.read_file(path=path, file_type=file_type, **kwargs)

    return Controller(reader)


def from_dataframe(df, log_level: str | int = INFO) -> Controller:
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

    if type(df) not in Controller.HANDLERS_TYPE_MAPPING.keys():
        raise ValueError(f'Unsupported object of type {type(df)}.')
    handler = Controller.HANDLERS_TYPE_MAPPING[type(df)]()

    logger.debug(f"Using {type(handler)} dataset handler")
    logger.info(f"Reading dataframe of type {type(df)}.")

    handler.read_dataframe(df)

    return Controller(handler)

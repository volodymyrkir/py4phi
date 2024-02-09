"""Module containing logic to process dataset configuration."""
import os
from configparser import DEFAULTSECT
from secrets import token_bytes

from configparser_crypt import ConfigParserCrypt

DEFAULT_SECRET_NAME = 'secret.key'  # TODO: CUSTOM CONFIGS
DEFAULT_CONFIG_NAME = 'decrypt.conf'


class ConfigProcessor:
    """Creates config based on parameters or reads config."""

    @staticmethod
    def __generate_key(path_to_save: str) -> bytes:
        key = token_bytes(32)
        with open(path_to_save, "wb") as key_file:
            key_file.write(key)
        return key

    @staticmethod
    def __read_key(path: str) -> bytes:
        try:
            with open(path, "rb") as key_file:
                key = key_file.read()
                return key
        except IsADirectoryError:
            raise IsADirectoryError(f'Provided path is a directory, not a file: {path}')
        except FileNotFoundError:
            raise FileNotFoundError(f'Secret not found under: {path}')  # TODO LOGGING

    def save_config(
            self,
            columns: dict[str, dict],
            path: str,
            encrypt_config: bool = True
    ) -> None:
        """
        Save config file based on column encryption parameters.

        Args:
        ----
        columns (dict[str, dict]): Encryption details dict.
        path (str): Path to save all configs.
                        Defaults to DEFAULT_PY4PHI_OUTPUT_PATH.
        encrypt_config (bool, optional): Whether to encrypt config itself.
                                            Defaults to True.

        Returns: None.

        """
        config = ConfigParserCrypt()
        for column_name, params_dict in columns.items():
            config[column_name] = params_dict

        if encrypt_config:
            key = self.__generate_key(os.path.join(path, DEFAULT_SECRET_NAME))
            config.aes_key = key
            with open(os.path.join(path, DEFAULT_CONFIG_NAME), "wb") as config_file:
                config.write_encrypted(config_file)
        else:
            with open(os.path.join(path, DEFAULT_CONFIG_NAME), "w") as config_file:
                config.write(config_file)

    def read_config(
            self,
            path: str,
            config_encrypted: bool = True,
            encryption_key_name: str = DEFAULT_SECRET_NAME
    ) -> dict[str, dict]:
        """
        Read config file.

        Args:
        ----
        path (str, optional): Path to save all configs.
                                Defaults to DEFAULT_PY4PHI_OUTPUT_PATH.
        config_encrypted (bool, optional): Whether config is encrypted.
                                            Defaults to True.
        encryption_key_name (str, optional): Name of the file
                                                with encryption key for config.

        Returns: None.

        """
        config = ConfigParserCrypt()
        if config_encrypted:
            key = self.__read_key(os.path.join(path, encryption_key_name))
            config.aes_key = key
            config.read_encrypted(os.path.join(path, DEFAULT_CONFIG_NAME))
        else:
            config.read(os.path.join(path, DEFAULT_CONFIG_NAME))
        return {
            column: dict(config.items(column))
            for column in config.sections()
            if column != DEFAULTSECT
        }









"""Module containing API for core functionality for py4phi."""
import os
from pathlib import Path
from logging import INFO

from py4phi.controller import Controller, ENGINE_NAME_MAPPING, HANDLERS_TYPE_MAPPING
from py4phi.logger_setup import logger
from py4phi.consts import PANDAS, POLARS, PYSPARK, DEFAULT_MODEL_KEY_PATH
from py4phi.config_processor import ConfigProcessor
from py4phi._encryption._model_encryptor import ModelEncryptor

__all__ = ['from_path', 'from_dataframe', 'encrypt_model', 'decrypt_model',
           'PANDAS', 'POLARS', 'PYSPARK']


def from_path(
        path: str,
        file_type: str,
        engine: str = PYSPARK,
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

    Raises: ValueError if specified engine is not supported.
    """
    logger.setLevel(log_level)

    logger.debug("Initializing Dataset Handler")
    reader_cls = ENGINE_NAME_MAPPING.get(engine)
    if not reader_cls:
        raise ValueError(f"No such engine: {engine}, "
                         f"can be one of {tuple(ENGINE_NAME_MAPPING)}")
    reader = reader_cls()
    logger.debug(f"Reading dataframe using {type(reader)} "
                 f"from file: {path}, of type {file_type}.")
    if not path.endswith(file_type):
        logger.warning(f"Pay attention, path {path} "
                       f"does not end with file type {file_type}")

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

    if type(df) not in HANDLERS_TYPE_MAPPING:
        raise ValueError(f'Unsupported object of type {type(df)}.')
    handler = HANDLERS_TYPE_MAPPING[type(df)]()

    logger.debug(f"Using {type(handler)} dataset handler")
    logger.info(f"Reading dataframe of type {type(df)}.")

    handler.read_dataframe(df)

    return Controller(handler)


def encrypt_model(
        path: str | Path,
        encrypt_config: bool = True
) -> None:
    """
    Encrypt model or folder and save config.

    Config is saved at the same level as model in 'py4phi_model_decryption' folder.

    Args:
    ----
    path (os.PathLike): Path to the directory with model components.
    encrypt_config (bool, optional): Whether to encrypt config additionally.
                                      Defaults to True.

    """
    config_processor = ConfigProcessor()
    conf_dict = ModelEncryptor.encrypt_folder(path)
    config_processor.save_config(
        conf_dict,
        os.path.join(Path(path).parent.absolute(), DEFAULT_MODEL_KEY_PATH),
        encrypt_config=encrypt_config
    )


def decrypt_model(
        path: str | Path,
        config_encrypted: bool = True
) -> None:
    """
    Decrypt model or folder.

    Args:
    ----
    path (os.PathLike): Path to the directory with model components.
    config_encrypted (bool, optional): Whether the config is encrypted additionally.
                                      Defaults to True.

    """
    config_processor = ConfigProcessor()
    conf_dict = config_processor.read_config(
        os.path.join(Path(path).parent.absolute(), DEFAULT_MODEL_KEY_PATH),
        config_encrypted=config_encrypted
    )
    try:
        key, aad = conf_dict['model']['key'], conf_dict['model']['aad']
    except KeyError:
        raise ValueError("Wrong decryption config. Missing key or aad.")
    ModelEncryptor.decrypt_folder(path, key, aad)

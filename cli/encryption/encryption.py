"""Module containing the encryption CLI commands."""
from pathlib import Path

import click

from py4phi.core import from_path, POLARS, PANDAS, PYSPARK

from cli.utils import parse_key_value
from cli.descriptions import (
    PATH_TO_INPUT_DESCRIPTION, FILE_TYPE_DESCRIPTION, COLUMN_TO_ENCRYPT,
    LOG_LEVEL_DESCRIPTION, ENGINE_TYPE, PATH_TO_OUTPUT_DESCRIPTION,
    PRINT_INTERMEDIATE, READ_OPTIONS, SAVE_FILE_TYPE,
    COLUMN_TO_DECRYPT, DISABLE_CONFIG_ENCRYPTION, CONFIG_NOT_ENCRYPTED, WRITE_OPTIONS
)
from py4phi.dataset_handlers.base_dataset_handler import PathOrStr


class EncryptionOptions(click.Command):
    """Encapsulates common options for encryption command group."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        arguments_options = [
            click.core.Option(('-l', '--log_level'),
                              help=LOG_LEVEL_DESCRIPTION,
                              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
                              default='INFO'),
            click.core.Option(('-i', '--input', 'input_path'),
                              help=PATH_TO_INPUT_DESCRIPTION,
                              type=click.Path(
                                  exists=True, readable=True, dir_okay=True
                              ),
                              required=True),
            click.core.Option(('-r', '--read_option', 'read_options'),
                              help=READ_OPTIONS,
                              type=(str, str),
                              callback=parse_key_value,
                              multiple=True),
            click.core.Option(('-t', '--file_type'),
                              help=FILE_TYPE_DESCRIPTION),
            click.core.Option(('-e', '--engine'),
                              help=ENGINE_TYPE,
                              type=click.Choice([PYSPARK, PANDAS, POLARS]),
                              default=PANDAS),
        ]
        for parameter in arguments_options:
            self.params.insert(0, parameter)


@click.group()
def encryption_group():
    """Encryption CLI commands group."""
    pass


@encryption_group.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
    cls=EncryptionOptions
)
@click.option('-p', '--print_intermediate',
              help=PRINT_INTERMEDIATE,
              is_flag=True)
@click.option('-c', '--columns_to_decrypt',
              help=COLUMN_TO_DECRYPT,
              multiple=True,
              required=True)
@click.option('-o', '--output_path',
              help=PATH_TO_OUTPUT_DESCRIPTION,
              type=click.Path(writable=True))
@click.option('--save_type',
              help=SAVE_FILE_TYPE)
@click.option('--config_not_encrypted',
              help=CONFIG_NOT_ENCRYPTED,
              is_flag=True)
@click.option('-w', '--write_option', 'write_options',
              help=WRITE_OPTIONS,
              type=(str, str),
              callback=parse_key_value,
              multiple=True)
def decrypt_and_save(
        input_path: PathOrStr,
        columns_to_decrypt: list[str],
        file_type: str,
        output_path: str,
        save_type: str,
        print_intermediate: bool,
        engine: str,
        log_level: str,
        config_not_encrypted: bool,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """Decrypt provided file using various options and save it."""
    opt_read_params = {
        **read_options,
        **{
            key: val for key, val in {
                'engine': engine,
                'log_level': log_level,
            }.items() if val
        }
    }
    opt_write_params = {
        **write_options,
        **{
            key: val for key, val in {
                'save_location': output_path,
                'save_format': save_type
            }.items() if val
        }
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.decrypt(
        configs_path=str(Path(input_path).parent.absolute()),
        columns_to_decrypt=columns_to_decrypt,
        config_encrypted=not config_not_encrypted
    )
    if print_intermediate:
        controller.print_current_df()
    controller.save_decrypted(**opt_write_params)


@encryption_group.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
    cls=EncryptionOptions
)
@click.option('-c', '--columns_to_decrypt',
              help=COLUMN_TO_DECRYPT,
              multiple=True,
              required=True)
@click.option('--config_not_encrypted',
              help=CONFIG_NOT_ENCRYPTED,
              is_flag=True)
def decrypt(
        input_path: PathOrStr,
        columns_to_decrypt: list[str],
        file_type: str,
        engine: str,
        log_level: str,
        config_not_encrypted: bool,
        read_options: dict[str, str],
):
    """
    Decrypt the input file and print it to the console for debugging purposes.

    This command should only be used for getting familiar with output format.
    Use decrypt-and-save to save encrypted data to the disk.
    """
    opt_read_params = {
        **read_options,
        **{
            key: val for key, val in {
                'engine': engine,
                'log_level': log_level,
            }.items() if val
        }
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.decrypt(
        configs_path=str(Path(input_path).parent.absolute()),
        columns_to_decrypt=columns_to_decrypt,
        config_encrypted=not config_not_encrypted
    )
    controller.print_current_df()


@encryption_group.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
    cls=EncryptionOptions
)
@click.option('-p', '--print_intermediate',
              help=PRINT_INTERMEDIATE,
              is_flag=True)
@click.option('-o', '--output_path',
              help=PATH_TO_OUTPUT_DESCRIPTION,
              type=click.Path(writable=True))
@click.option('-c', '--columns_to_encrypt',
              help=COLUMN_TO_ENCRYPT,
              multiple=True,
              required=True)
@click.option('--disable_config_encryption',
              help=DISABLE_CONFIG_ENCRYPTION,
              is_flag=True)
@click.option('-w', '--write_option', 'write_options',
              help=WRITE_OPTIONS,
              type=(str, str),
              callback=parse_key_value,
              multiple=True)
def encrypt_and_save(
        input_path: PathOrStr,
        columns_to_encrypt: list[str],
        file_type: str,
        output_path: str,
        print_intermediate: bool,
        engine: str,
        log_level: str,
        disable_config_encryption: bool,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """Encrypt an input file and save it."""
    opt_read_params = {
        **read_options,
        **{key: val for key, val in {
            'engine': engine,
            'log_level': log_level,
        }.items() if val}
    }
    opt_write_params = {
        **write_options,
        **{key: val for key, val in {
            'save_location': output_path,
        }.items() if val}
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.encrypt(columns_to_encrypt=columns_to_encrypt)
    if print_intermediate:
        controller.print_current_df()
    controller.save_encrypted(
        encrypt_config=not disable_config_encryption,
        **opt_write_params
    )


@encryption_group.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
    cls=EncryptionOptions
)
@click.option('-c', '--columns_to_encrypt',
              help=COLUMN_TO_ENCRYPT,
              multiple=True,
              required=True)
def encrypt(
        input_path: PathOrStr,
        columns_to_encrypt: list[str],
        file_type: str,
        engine: str,
        log_level: str,
        read_options: dict[str, str],
):
    """
    Encrypt an input file and print it to the console for debugging purposes.

    This command should only be used for getting familiar with output format.
    Use encrypt-and-save to save encrypted data to disk.
    """
    opt_read_params = {
        **read_options,
        **{key: val for key, val in {
            'engine': engine,
            'log_level': log_level,
        }.items() if val}
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.encrypt(columns_to_encrypt=columns_to_encrypt)

    controller.print_current_df()

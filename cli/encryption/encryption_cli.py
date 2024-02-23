"""This module contains the encryption CLI commands."""

import click

from py4phi.controller import *

from cli.descriptions import (
    PATH_TO_INPUT_DESCRIPTION, FILE_TYPE_DESCRIPTION, COLUMN_TO_ENCRYPT,
    LOG_LEVEL_DESCRIPTION, ENGINE_TYPE, PATH_TO_OUTPUT_DESCRIPTION, PRINT_INTERMEDIATE, READ_OPTIONS, SAVE_FILE_TYPE,
    COLUMN_TO_DECRYPT
)
from py4phi.dataset_handlers.base_dataset_handler import PathOrStr


def parse_key_value(*args):
    """Utility function to parse read/write key-value booleans."""
    opts = dict(args[-1])
    d = {
        key: val.lower() == 'true'
        if val.lower() in ('true', 'false') else val
        for key, val in opts.items()
    }
    return d


class EncryptionOptions(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        arguments_options = [
            click.core.Option(('-l', '--log_level'),
                              help=LOG_LEVEL_DESCRIPTION,
                              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
                              default='INFO'),
            click.core.Option(('-i', '--input', 'input_path'),
                              help=PATH_TO_INPUT_DESCRIPTION,
                              type=click.Path(exists=True, readable=True, dir_okay=True),
                              required=True),
            click.core.Option(('-o', '--output_path'),
                              help=PATH_TO_OUTPUT_DESCRIPTION,
                              type=click.Path(writable=True)),
            click.core.Option(('-p', '--print_intermediate'),
                              help=PRINT_INTERMEDIATE,
                              is_flag=True),
            click.core.Option(('-r', '--read_option', 'read_options'),
                              help=READ_OPTIONS,
                              type=(str, str),
                              callback=parse_key_value,
                              multiple=True),
            click.core.Option(('-w', '--write_option', 'write_options'),
                              help=READ_OPTIONS,
                              type=(str, str),
                              callback=parse_key_value,
                              multiple=True),
        ]
        for parameter in arguments_options:
            self.params.insert(0, parameter)


@click.group()
def encryption_group():
    pass


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
@click.option('-t', '--file-type',
              help=FILE_TYPE_DESCRIPTION)
@click.option('-e', '--engine',
              help=ENGINE_TYPE,
              type=click.Choice([PYSPARK, PANDAS, POLARS]),
              default=PYSPARK)
def encrypt_and_save(
        input_path: PathOrStr,
        columns_to_encrypt: list[str],
        file_type: str,
        output_path: str,
        print_intermediate: bool,
        engine: str,
        log_level: str,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """This command encrypts your dataframe, and saves it."""
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
    controller.save_encrypted(**opt_write_params)


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
@click.option('-t', '--file-type',
              help=FILE_TYPE_DESCRIPTION)
@click.option('--save_type',
              help=SAVE_FILE_TYPE)
@click.option('-e', '--engine',
              help=ENGINE_TYPE,
              type=click.Choice([PYSPARK, PANDAS, POLARS]),
              default=PYSPARK)
def decrypt_and_save(
        input_path: PathOrStr,
        columns_to_decrypt: list[str],
        file_type: str,
        output_path: str,
        save_type: str,
        print_intermediate: bool,
        engine: str,
        log_level: str,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """This command encrypts your dataframe, and saves it."""
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
        **read_options,
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
    controller.decrypt(columns_to_decrypt=columns_to_decrypt)
    if print_intermediate:
        controller.print_current_df()
    controller.save_decrypted(**opt_write_params)

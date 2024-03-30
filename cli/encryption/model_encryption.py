"""Module containing the model/folder encryption CLI commands."""

import click

from py4phi.core import encrypt_model as enc_m, decrypt_model as dec_m

from cli.descriptions import (
    DISABLE_CONFIG_ENCRYPTION, INPUT_PATH_FOLDER,
    INPUT_ENCRYPTED_FOLDER, CONFIG_NOT_ENCRYPTED
)
from py4phi.dataset_handlers.base_dataset_handler import PathOrStr


@click.group()
def model_encryption_group():
    """Model/Folder encryption CLI commands group."""
    pass


@model_encryption_group.command()
@click.option('-p', '--path',
              help=INPUT_PATH_FOLDER,
              type=click.Path(
                  readable=True,
                  writable=True,
                  exists=True,
                  dir_okay=True,
                  resolve_path=True
              ),
              required=True)
@click.option('-d', '--disable_config_encryption',
              help=DISABLE_CONFIG_ENCRYPTION,
              is_flag=True)
def encrypt_model(
        path: PathOrStr,
        disable_config_encryption: bool
):
    """Encrypt ML models or other folders recursively."""
    enc_m(path, encrypt_config=not disable_config_encryption)


@model_encryption_group.command()
@click.option('-p', '--path',
              help=INPUT_ENCRYPTED_FOLDER,
              type=click.Path(
                  readable=True,
                  writable=True,
                  exists=True,
                  dir_okay=True,
                  resolve_path=True
              ),
              required=True)
@click.option('-c', '--config_not_encrypted',
              help=CONFIG_NOT_ENCRYPTED,
              is_flag=True)
def decrypt_model(
        path: PathOrStr,
        config_not_encrypted: bool
):
    """Decrypt ML models or other folders recursively."""
    dec_m(path, config_encrypted=not config_not_encrypted)

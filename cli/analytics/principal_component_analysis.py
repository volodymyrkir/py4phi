"""Module that contains the principal component analysis CLI commands."""

import click

from py4phi.core import from_path, POLARS, PANDAS, PYSPARK

from cli.utils import parse_key_value
from cli.descriptions import (
    PATH_TO_INPUT_DESCRIPTION, PATH_TO_OUTPUT_DESCRIPTION,
    WRITE_OPTIONS, TARGET_FEATURE, COLUMNS_TO_IGNORE,
    SAVE_REDUCED, CUM_VAR_THRESHOLD, SAVE_FILE_TYPE,
    FILE_TYPE_DESCRIPTION, ENGINE_TYPE, READ_OPTIONS,
    NULLS_MODE, NUM_COMPONENTS
)
from py4phi.dataset_handlers.base_dataset_handler import PathOrStr


@click.group()
def pca_group():
    """Group of commands related to principal component analysis."""
    pass


@pca_group.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option('-i', '--input', 'input_path',
              help=PATH_TO_INPUT_DESCRIPTION,
              type=click.Path(exists=True, readable=True, dir_okay=True),
              required=True)
@click.option('-t', '--file_type',
              help=FILE_TYPE_DESCRIPTION)
@click.option('--target', 'target_feature',
              help=TARGET_FEATURE)
@click.option('-c', '--columns_to_ignore',
              help=COLUMNS_TO_IGNORE,
              multiple=True)
@click.option('-s', '--save_reduced',
              help=SAVE_REDUCED,
              is_flag=True)
@click.option('--cum_var_threshold',
              help=CUM_VAR_THRESHOLD,
              type=click.FLOAT)
@click.option('--num_components',
              help=NUM_COMPONENTS,
              type=click.INT)
@click.option('--nulls_mode',
              help=NULLS_MODE,
              type=click.Choice(['drop', 'fill']))
@click.option('-o', '--output_path',
              help=PATH_TO_OUTPUT_DESCRIPTION,
              type=click.Path(writable=True))
@click.option('--save_type',
              help=SAVE_FILE_TYPE)
@click.option('-e', '--engine',
              help=ENGINE_TYPE,
              type=click.Choice([PYSPARK, PANDAS, POLARS]),
              default=PANDAS)
@click.option('-r', '--read_option', 'read_options',
              help=READ_OPTIONS,
              type=(str, str),
              callback=parse_key_value,
              multiple=True)
@click.option('-w', '--write_option', 'write_options',
              help=WRITE_OPTIONS,
              type=(str, str),
              callback=parse_key_value,
              multiple=True)
def perform_pca(
        input_path: PathOrStr,
        file_type: str,
        target_feature: str,
        columns_to_ignore: list[str],
        save_reduced: bool,
        cum_var_threshold: float,
        num_components: int,
        nulls_mode: str,
        output_path: str,
        save_type: str,
        engine: str,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """Perform principal component analysis using Pandas and scikit-learn."""
    opt_read_params = {
        **read_options,
        **{
            key: val for key, val in {
                'engine': engine,
            }.items() if val
        }
    }
    opt_write_params = {
        **write_options,
        **{
            key: val for key, val in {
                'save_folder': output_path,
                'save_format': save_type,
                'rec_threshold': cum_var_threshold,
                'n_components': num_components,
                'nulls_mode': nulls_mode
            }.items() if val
        }
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.perform_pca(
        target_feature=target_feature,
        ignore_columns=columns_to_ignore,
        save_reduced=save_reduced,
        **opt_write_params
    )

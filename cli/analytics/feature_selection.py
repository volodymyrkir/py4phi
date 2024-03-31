"""Module containing CLI commands for feature selection."""

import click

from py4phi.core import from_path, POLARS, PANDAS, PYSPARK

from cli.utils import parse_key_value
from cli.descriptions import (
    PATH_TO_INPUT_DESCRIPTION, PATH_TO_OUTPUT_DESCRIPTION,
    WRITE_OPTIONS, TARGET_FEATURE,  SAVE_FILE_TYPE,
    FILE_TYPE_DESCRIPTION, ENGINE_TYPE, READ_OPTIONS,
    SAVE_REDUCED_FEATURES, OVERRIDE_DROP_COLUMN,
    TARGET_COR_THRESHOLD, FEATURE_COR_THRESHOLD
)
from py4phi.dataset_handlers.base_dataset_handler import PathOrStr


@click.group()
def feature_selection_group():
    """Group of commands related to principal component analysis."""
    pass


@feature_selection_group.command(
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
              help=TARGET_FEATURE,
              required=True)
@click.option('-c', '--override_column_to_drop',
              help=OVERRIDE_DROP_COLUMN,
              multiple=True)
@click.option('-s', '--save_reduced',
              help=SAVE_REDUCED_FEATURES,
              is_flag=True)
@click.option('--target_corr_threshold',
              help=TARGET_COR_THRESHOLD,
              type=click.FLOAT)
@click.option('--feature_corr_threshold',
              help=FEATURE_COR_THRESHOLD,
              type=click.FLOAT)
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
def feature_selection(
        input_path: PathOrStr,
        file_type: str,
        target_feature: str,
        override_column_to_drop: list[str],
        save_reduced: bool,
        target_corr_threshold: float,
        feature_corr_threshold: float,
        output_path: str,
        save_type: str,
        engine: str,
        read_options: dict[str, str],
        write_options: dict[str, str]
):
    """Perform feature selection using Pearson correlation analysis.."""
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
                'drop_recommended': save_reduced,
                'target_correlation_threshold': target_corr_threshold,
                'features_correlation_threshold': feature_corr_threshold,
                'override_columns_to_drop': override_column_to_drop
            }.items() if val
        }
    }
    if not file_type:
        file_type = input_path.split('.')[-1]

    controller = from_path(input_path, file_type, **opt_read_params)
    controller.perform_feature_selection(
        target_feature=target_feature,
        **opt_write_params
    )

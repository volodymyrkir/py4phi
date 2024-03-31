"""Entrypoint module for py4phi CLI."""
import click

from cli.encryption.encryption import encryption_group
from cli.encryption.model_encryption import model_encryption_group
from cli.analytics.principal_component_analysis import pca_group
from cli.analytics.feature_selection import feature_selection_group

main = click.CommandCollection(
    sources=[
        encryption_group, model_encryption_group,
        pca_group, feature_selection_group
    ]
)


if __name__ == '__main__':
    main()

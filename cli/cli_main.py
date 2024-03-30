"""Entrypoint module for py4phi CLI."""
import click

from cli.encryption.encryption import encryption_group
from cli.encryption.model_encryption import model_encryption_group
from cli.analytics.principal_component_analysis import pca_group


main = click.CommandCollection(
    sources=[encryption_group, model_encryption_group, pca_group]
)


if __name__ == '__main__':
    main()

import click

from cli.encryption.encryption import encryption_group
from cli.encryption.model_encryption import model_encryption_group


main = click.CommandCollection(sources=[encryption_group, model_encryption_group])


if __name__ == '__main__':
    main()


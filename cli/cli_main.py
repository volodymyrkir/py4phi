import click

from cli.encryption.encryption_cli import encryption_group


main = click.CommandCollection(sources=[encryption_group, ])


if __name__ == '__main__':
    main()


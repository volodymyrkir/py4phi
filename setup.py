from setuptools import setup, find_packages


def parse_requirements(filename):
    """Parses and returns list of requirements from given file."""
    with open(filename, 'r') as f:
        return [requirement for requirement in f.read().splitlines()]


setup(
    name='py4phi',
    version='0.1.0',
    packages=find_packages(exclude=['tests.*', 'tests']),
    package_data={"": ["*log4j.properties"]},
    include_package_data=True,
    install_requires=parse_requirements('requirements.txt'),
    entry_points={
        'console_scripts': [
            'py4phi=cli.cli_main:main',
        ],
    },

)
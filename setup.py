from setuptools import setup, find_packages

setup(
    name='py4phi',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'iniconfig==2.0.0',
        'pluggy==1.3.0',
        'pyspark==3.5.0',
        'configparser-crypt==1.1.0',
        'pycryptodome==3.20.0',
        'pandas==2.2.0',
        'pyarrow==15.0.0',
        'polars==0.20.9',
        'click==8.1.7'
    ],
    entry_points={
        'console_scripts': [
            'py4phi=cli.cli_main:main',
        ],
    },

)
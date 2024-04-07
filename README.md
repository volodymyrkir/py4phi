# Introduction
**py4phi** is a simple solution for the complex problem of dealing with sensitive data.

In the modern IT world, sharing a dataset with sensitive data is common, especially if a team working on it is wide. It can be used for various purposes, including building a ML/DL model, simple business analysis, etc. Of course, in most companies, different restrictions are applied on the data, including row-level security, column hashing, or encrypting, but this requires at least some knowledge of data engineering libraries and can be  a challenging and time-consuming task. At the same time, employees with access to sensitive parts of the data may not have such expertise, which is where **py4phi** can be helpful.

[![Coverage Status](https://coveralls.io/repos/github/volodymyrkir/py4phi/badge.svg?branch=ci_update)](https://coveralls.io/github/volodymyrkir/py4phi?branch=ci_update)
![PyPI - Downloads](https://img.shields.io/pypi/dm/py4phi)
[![image](https://img.shields.io/pypi/v/py4phi.svg)](https://pypi.python.org/pypi/py4phi)
[![image](https://img.shields.io/pypi/l/py4phi.svg)](https://pypi.python.org/pypi/py4phi)
[![image](https://img.shields.io/pypi/pyversions/py4phi.svg)](https://pypi.python.org/pypi/py4phi)
# Functionality
**py4phi** offers the following functionality to solve the problem mentioned above and more:
**Encrypt a dataset column-wise.**
**Decrypt a dataset column-wise.**
**Encrypt any folder or machine learning model**
**Decrypt any folder or machine learning model**
**Perform principal component analysis on a dataset**
**Perform correlation analysis for feature selection on a dataset**
You can use **py4phi** both in Python code and through your terminal via the convenient CLI interface.
#Setup and prerequisites
In order to install the library [from PyPi](), just run
```shell
pip install py4phi
```

### **py4phi** is compatible with the following engines for data processing and encryption:
* [Pandas](https://github.com/pandas-dev/pandas)
* [PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
* [Polars](https://pola.rs/) 

NOTE: Default engine for CLI is Pandas, whereas for library - PySpark.

Nevertheless, you'll need a JDK installed and a JAVA_HOME environment variable set in order 
to work with the PySpark engine. 
However, you can still work with Pandas or Polars without JDK, if you wish.
# Usage
You can integrate **py4phi** in your existing pandas/pyspark/polars data pipeline by 
initializing from a DataFrame or loading from a file. Currently, CSV and Parquet 
file types are supported. 

Encryption and decryption of the datasets are facilitated by the use of configs.
Each column gets its own encryption key and a nonce, which are saved in the configs. These resulting files can be further encrypted for even more safety.

Therefore, you can encrypt only sensitive columns, send the outputs, for example, to the data analysis team,
and keep the data safe. Later, data can be decrypted using configs on-demand.
Moreover, you do not need deep knowledge of the underlying engines (pandas, etc.)
and don't need to write long scripts to encrypt data and save the keys. 

The following example showcases the encryption process of a dataset.csv file. \
(you can find it in the [/examples](https://github.com/volodymyrkir/py4phi/tree/main/examples) folder) \
The output dataset, along with the decryption configs, is then saved to the "test_folder" directory under CWD,

```python
from py4phi.core import from_path

controller = from_path(
    './dataset.csv',
    'csv',
    engine='pyspark',
    log_level='DEBUG',
    header=True  # pyspark read option
)
controller.print_current_df()
controller.encrypt(columns_to_encrypt=['Staff involved', 'ACF'])
controller.print_current_df()
controller.save_encrypted(
    output_name='my_encrypted_file',
    save_location='./test_folder/',   # results will be saved under CWD/test_folder/py4phi_encrypted_outputs
    save_format='PARQUET',
)

```

To decrypt these outputs, you can use:
```python
import pandas as pd
from py4phi.core import from_dataframe

df = pd.read_parquet('./test_folder/py4phi_encrypted_outputs/my_encrypted_file.parquet')
controller = from_dataframe(
    df,
    log_level='DEBUG'
)
controller.print_current_df()
controller.decrypt(
    columns_to_decrypt=['Staff involved', 'ACF'],
    configs_path='./test_folder/py4phi_encrypted_outputs', 
)
controller.print_current_df()
controller.save_decrypted(
    output_name='my_decrypted_file',
    save_location='./test_folder',
    save_format='csv'
)
```

This example also shows how to initialize **py4phi** from a (pandas, in this case) DataFrame.

Similar workflow through a terminal can be executed with the following CLI commands:
```shell
py4phi encrypt-and-save -i ./dataset.csv -c ACF -c 'Staff involved' -e pyspark -p -o ./ -r header True
py4phi decrypt-and-save -i ./py4phi_encrypted_outputs/output_dataset.csv -e pyspark -c ACF -c 'Staff involved' -p -o ./ -r header True
```

To encrypt and decrypt a folder or a ML/DL model, you can use:
```python
from py4phi.core import encrypt_model, decrypt_model
encrypt_model(
    './test_folder',
    encrypt_config=False #or True
)

decrypt_model(
    './test_folder',
    config_encrypted=False # or True
)
```
After encryption, all files whithin the specified folder will be not readable.
This can be used for easy one-line model encryption.

The same actions can be taken in a terminal:
```shell
# encrypt model/folder, do not encrypt config. Note that encryption is done inplace. Please save original before encryption.
py4phi encrypt-model -p ./py4phi_encrypted_outputs/ -d

# decrypt model/folder when config is not encrypted
py4phi decrypt-model -p ./py4phi_encrypted_outputs/ -c
```
# Analytics usage
Apart from the main encrypt/decrypt functionality, one may be interested in reducing
the dimensionality of a dataset or performing correlation analysis of the feature (feature selection).
In a typical scenario, this requires a lot of effort from the data analyst.
Instead, a person with access to the sensitive data 
can perform a lightweight **PCA/feature selection** in a couple of code lines or terminal commands.

**NOTE**: This functionality is a quick top-level analysis, diving deeper into a dataset's feature analysis will always bring more profit.

To perform principal component analysis with Python, use: 

```python
from py4phi.core import from_path
controller = from_path('Titanic.parquet', file_type='parquet', engine='pyspark')
controller.perform_pca(
    target_feature='Survived',
    ignore_columns=['Name', 'Sex', 'Ticket', 'Cabin', 'Embarked'],
    save_reduced=False
)
```

Via terminal:

```shell
py4phi perform-pca -i ./dataset.csv  --target 'Staff involved' -c ACF
```

To perform feature selection with Python, use: 

```python
from py4phi.core import from_path
controller = from_path('Titanic.parquet', file_type='parquet', engine='polars')
controller.perform_feature_selection(
    target_feature='Survived',
    target_correlation_threshold=0.2,
    features_correlation_threshold=0.2,
    drop_recommended=False
)
```

Via terminal:

```shell
py4phi feature-selection -i ./Titanic.parquet --target Survived --target_corr_threshold 0.3 --feature_corr_threshold 0.55
```

Please look into the [/examples](https://github.com/volodymyrkir/py4phi/tree/main/examples) folder for more examples.
It also contains training datasets.

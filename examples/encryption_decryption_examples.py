from pyspark.sql import SparkSession, functions as f

from py4phi.core import from_dataframe, from_path

# 1) load from a csv file, encrypt, print intermediate result and save as a parquet file with default encrypted configs
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
    save_location='./test_folder/',  # results will be saved under CWD/test_folder/py4phi_encrypted_outputs
    save_format='PARQUET',
)

# 2) load from dataframe, encrypt intermediate, print and save WITHOUT encrypting decryption configs
controller = from_dataframe(df, log_level='DEBUG')
controller.print_current_df()
controller.encrypt(columns_to_encrypt=['Staff involved', 'ACF'])
controller.print_current_df()
controller.save_encrypted(
    output_name='my_encrypted_dataframe',
    save_location='./test_folder2/',  # results will be saved under CWD/test_folder2/py4phi_encrypted_outputs
    save_format='csv',
    encrypt_config=False,
    index=False,  # Pandas csv write option
    header=1  # Pandas csv write options
)

# 3) load the previously encrypted file from a pyspark dataframe,
# rename column, decrypt and map old column to new,
# print intermediate result and save at the same folder
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet('./test_folder/py4phi_encrypted_outputs/my_encrypted_file.parquet')
df = (
    df.withColumn('ACFNEW', f.col('ACF'))
    .drop('ACF')
)
controller = from_dataframe(
    df,
    log_level='DEBUG'
)
controller.print_current_df()
controller.decrypt(
    columns_to_decrypt=['Staff involved', 'ACF'],
    configs_path='./test_folder/py4phi_encrypted_outputs',  # provide path where decryption configs reside
    columns_mapping={'ACF': 'ACFNEW'}   # This feature is not available in the CLI yet
)
controller.print_current_df()
controller.save_decrypted(
    output_name='my_decrypted_file',
    save_location='./test_folder',
    save_format='csv'
)

#
# # 4) load the previously encrypted pandas df with polars
# # decrypt, print intermediate result and save at the same folder in the parquet format
controller = from_path(
    './test_folder2/py4phi_encrypted_outputs/my_encrypted_dataframe.csv',
    'csv',
    engine='polars',
    log_level='DEBUG',
)
controller.print_current_df()
controller.decrypt(
    columns_to_decrypt=['Staff involved', 'ACF'],
    configs_path='./test_folder2/py4phi_encrypted_outputs',  # provide path where decryption config resides
    config_encrypted=False  # encrypt_config=False was set above
)
controller.print_current_df()
controller.save_decrypted(
    output_name='my_decrypted_file',
    save_location='./test_folder2',
    save_format='parquet'
)

# 5) No-saving flow to only demonstrate encryption and decryption correctness
controller = from_path(
    './Titanic.parquet',
    'parquet',
    engine='polars',  # or pyspark or pandas
    log_level='DEBUG'
)

controller.encrypt(columns_to_encrypt=['Embarked', 'Age', 'Survived'])
controller.print_current_df()
controller.decrypt(columns_to_decrypt=['Embarked', 'Age'])
controller.print_current_df()
# IMPORTANT NOTE: for structured files with schema (parquet), types for encrypted columns will be CHANGED to string,
# please cast them back to necessary types if you need it.
# You can either get dataframe from controller without saving it and perform casting with
# df = controller._current_df
# OR save encrypted/decrypted file and then load it back in your code and perform data casting.

"""Contains logic for _encryption and decryption."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as f, types as t

from py4phi._encryption._encryptor import _BaseEncryptor


class _PySparkEncryptor(_BaseEncryptor):
    """Encryptor class, includes _encryption and decryption logic."""

    def _encrypt_column(self, column: str) -> DataFrame:
        """
        Encrypt dataframe column.

        Args:
        ----
            column (str): The column to be encrypted.

        Returns: PySpark DataFrame with encrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")
        self._get_and_save_salt(column)
        return (self._df
                .withColumn(column,
                            f.base64(
                                f.aes_encrypt(
                                            input=f.col(column),
                                            key=f.lit(self._columns[column]['key']),
                                            aad=f.lit(self._columns[column]['aad'])
                                )
                            )
                            )
                )

    def _decrypt_column(self, column: str) -> DataFrame:
        """
        Decrypt dataframe column.

        Args:
        ----
            column (str): The column to be decrypted.

        Returns: PySpark DataFrame with decrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")

        return (self._df
                .withColumn(column,
                            f.aes_decrypt(
                                    input=f.unbase64(f.col(column)),
                                    key=f.lit(self._columns[column]['key']),
                                    aad=f.lit(self._columns[column]['aad'])
                                ).cast(t.StringType())
                            )
                )


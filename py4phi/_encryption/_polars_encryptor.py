"""Contains logic for encryption and decryption using Polars."""
import polars as pl

from py4phi._encryption._encryptor import _BaseEncryptor


class _PolarsEncryptor(_BaseEncryptor):
    """Encryptor class, includes _encryption and decryption logic."""

    def _encrypt_column(self, column: str) -> pl.DataFrame:
        """
        Encrypt dataframe column.

        Args:
        ----
            column (str): The column to be encrypted.

        Returns: Polars DataFrame with encrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")
        if column not in self._columns:
            raise ValueError(f"No column named {column} found in encryption dict.")
        self._get_and_save_salt(column)
        key = bytes.fromhex(self._columns[column]['key'])
        aad = bytes.fromhex(self._columns[column]['aad'])

        return self._df.with_columns(
            [
                pl.col(column).map_elements(
                    lambda x: self._encrypt_string(str(x), key, aad),
                    return_dtype=pl.String
                ).alias(column)
            ]
        )

    def _decrypt_column(
            self,
            column: str,
            decryption_dict: dict[str, None | str]
    ) -> pl.DataFrame:
        """
        Decrypt dataframe column.

        Args:
        ----
        column (str): The column to be decrypted.
        decryption_dict (dict[str, dict]): The decryption dictionary
                                            with key and aad keys.

        Returns: Polars DataFrame with decrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")

        key = bytes.fromhex(decryption_dict['key'])
        aad = bytes.fromhex(decryption_dict['aad'])

        return self._df.with_columns(
            [
                pl.col(column).map_elements(
                    lambda x: self._decrypt_string(x, key, aad),
                    return_dtype=pl.String
                ).alias(column)
            ]
        )


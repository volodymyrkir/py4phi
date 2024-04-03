"""Contains logic for _encryption and decryption."""
import pandas as pd

from py4phi._encryption._encryptor import _BaseEncryptor


class _PandasEncryptor(_BaseEncryptor):
    """Encryptor class, includes _encryption and decryption logic."""

    def _encrypt_column(self, column: str) -> pd.DataFrame:
        """
        Encrypt dataframe column.

        Args:
        ----
            column (str): The column to be encrypted.

        Returns: Pandas DataFrame with encrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")
        if column not in self._columns:
            raise ValueError(f"No column named {column} found in encryption dict.")
        self._get_and_save_salt(column)
        key = bytes.fromhex(self._columns[column]['key'])
        aad = bytes.fromhex(self._columns[column]['aad'])

        self._df[column] = self._df[column].apply(
            lambda x: self._encrypt_string(str(x), key, aad)
        )
        return self._df

    def _decrypt_column(
            self,
            column: str,
            decryption_dict: dict[str, None | str]
    ) -> pd.DataFrame:
        """
        Decrypt dataframe column.

        Args:
        ----
        column (str): The column to be decrypted.
        decryption_dict (dict[str, dict]): The decryption dictionary
                                            with key and aad keys.

        Returns: PySpark DataFrame with decrypted column.

        """
        if column not in self._df.columns:
            raise ValueError(f"No column named {column} found in file provided.")

        key = bytes.fromhex(decryption_dict['key'])
        aad = bytes.fromhex(decryption_dict['aad'])

        self._df[column] = self._df[column].apply(
            lambda x: self._decrypt_string(x, key, aad)
        )
        return self._df


"""Contains logic for _encryption and decryption."""
from typing import override
import base64

import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from py4phi._encryption._encryptor import _BaseEncryptor


class _PandasEncryptor(_BaseEncryptor):
    """Encryptor class, includes _encryption and decryption logic."""

    @staticmethod
    def _encrypt_string(data: str, key: bytes, aad: bytes) -> str:
        """
        Encrypts a string using AES encryption with a given key and AAD.

        Args:
        ----
        data (str): The string to encrypt.
        key (bytes): The 16-byte encryption key.
        aad (bytes): The AAD (Additional Authenticated Data).

        Returns: (str) The encrypted data.
        """
        cipher = AES.new(key, AES.MODE_GCM, nonce=aad)
        ciphertext, tag = cipher.encrypt_and_digest(pad(data.encode(), AES.block_size))
        return base64.b64encode(cipher.nonce + tag + ciphertext).decode()

    @staticmethod
    def _decrypt_string(data: bytes, key: bytes, aad: bytes) -> str:
        """
        Decrypts an encrypted string using AES decryption with a given key and AAD.

        Args:
        ----
        data (bytes): The encrypted data.
        key (bytes): The 16-byte encryption key.
        aad (bytes): The AAD (Additional Authenticated Data).

        Returns: (str) The decrypted string.
        """
        data = base64.b64decode(data)
        tag = data[16:32]
        ciphertext = data[32:]

        # Create AES cipher with nonce and tag
        cipher = AES.new(key, AES.MODE_GCM, nonce=aad)
        decrypted_data = unpad(
            cipher.decrypt_and_verify(ciphertext, tag),
            AES.block_size
        )

        return decrypted_data.decode()

    @override
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

    @override
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

        key = bytes.fromhex(self._columns[column]['key'])
        aad = bytes.fromhex(self._columns[column]['aad'])

        self._df[column] = self._df[column].apply(
            lambda x: self._decrypt_string(x, key, aad)
        )
        return self._df


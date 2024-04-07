"""Contains logic for _encryption and decryption."""
from abc import ABC, abstractmethod
from typing import Generic, Dict, Union
from secrets import token_hex
from base64 import b64encode, b64decode

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from py4phi.consts import STR_TAG_START, STR_TAG_END
from py4phi.logger_setup import logger
from py4phi.utils import DataFrame

ColumnsDict = Dict[str, Dict[str, Union[None, str]]]


class _BaseEncryptor(ABC, Generic[DataFrame]):
    """Encryptor class, includes _encryption and decryption logic."""

    def __init__(self, df: DataFrame, columns: list[str]):
        self._df: DataFrame = df
        self._columns: ColumnsDict = {
            column: dict.fromkeys(['key', 'aad'])
            for column in columns
        }

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
        return b64encode(cipher.nonce + tag + ciphertext).decode()

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
        data = b64decode(data)
        tag = data[STR_TAG_START:STR_TAG_END]
        ciphertext = data[STR_TAG_END:]

        cipher = AES.new(key, AES.MODE_GCM, nonce=aad)
        decrypted_data = unpad(
            cipher.decrypt_and_verify(ciphertext, tag),
            AES.block_size
        )

        return decrypted_data.decode()

    @abstractmethod
    def _encrypt_column(self, column: str) -> DataFrame:
        """
        Encrypt dataframe column.

        Args:
        ----
            column (str): The column to be encrypted.

        Returns: DataFrame with encrypted column.

        """

    @abstractmethod
    def _decrypt_column(
            self,
            column: str,
            decryption_dict: dict[str, None | str]
    ) -> DataFrame:
        """
        Decrypt dataframe column.

        Args:
        ----
        column (str): The column to be decrypted.
        decryption_dict (dict[str, dict]): The decryption dictionary
                                            with key and aad keys.

        Returns: DataFrame with decrypted column.

        """

    def encrypt(self) -> tuple[DataFrame, ColumnsDict]:
        """
        Encrypt each dataframe column.

        Returns: Encrypted dataframe and columns dict with decryption details.

        """
        for column in self._columns:
            logger.info(f"Encrypting column: {column}.")
            self._df = self._encrypt_column(column)
        return self._df, self._columns

    def decrypt(self, decryption_dict: ColumnsDict) -> DataFrame:
        """
        Encrypt each dataframe column.

        Args:
        ----
        decryption_dict (dict[str, dict]): Dictionary containing
                                            decryption details for each column.

        Returns: Decrypted dataframe.

        """
        for column in self._columns:
            logger.info(f"Decrypting column: {column}.")
            if column in decryption_dict:
                self._df = self._decrypt_column(column, decryption_dict[column])
            else:
                logger.error(f"Column {column} is not in decryption config,"
                             f" cannot decrypt it.")
        return self._df

    def _get_and_save_salt(self, column: str) -> None:
        """
        Get aad and key salt, save them for particular column.

        Args:
        ----
            column (str): The column to be salted.

        Returns: None

        """
        if column not in self._columns.keys():
            raise ValueError(f"No column in encryptor columns dict, {column}")
        self._columns[column]['key'] = token_hex(16)
        self._columns[column]['aad'] = token_hex(16)

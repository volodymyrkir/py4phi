"""Contains logic for _encryption and decryption."""
from abc import ABC, abstractmethod
from secrets import token_hex


class _BaseEncryptor(ABC):
    """Encryptor class, includes _encryption and decryption logic."""

    def __init__(self, df, columns: list[str]):
        self._df = df
        self._columns = {
            column: dict.fromkeys(['key', 'aad'])
            for column in columns
        }

    @abstractmethod
    def _encrypt_column(self, column: str):
        """
        Encrypt dataframe column.

        Args:
        ----
            column (str): The column to be encrypted.

        Returns: DataFrame with encrypted column.

        """

    @abstractmethod
    def _decrypt_column(self, column: str, decryption_dict: dict[str, str]):
        """
        Decrypt dataframe column.

        Args:
        ----
        column (str): The column to be decrypted.
        decryption_dict (dict[str, dict]): The decryption dictionary
                                            with key and aad keys.

        Returns: DataFrame with decrypted column.

        """

    def encrypt(self):
        """
        Encrypt each dataframe column.

        Returns: Encrypted dataframe and columns dict with decryption details.

        """
        for column in self._columns:
            self._df = self._encrypt_column(column)
        return self._df, self._columns

    def decrypt(self, decryption_dict: dict[str, dict]):
        """
        Encrypt each dataframe column.

        Args:
        ----
        decryption_dict (dict[str, dict]): Dictionary containing
                                            decryption details for each column.

        Returns: Decrypted dataframe.

        """
        for column in self._columns:
            self._df = self._decrypt_column(column, decryption_dict[column])
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
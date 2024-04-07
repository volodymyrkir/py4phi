"""Module for encryption of the ML models."""
import os
from base64 import b64encode, b64decode
from secrets import token_hex

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad

from py4phi.logger_setup import logger
from py4phi.utils import PathOrStr


class ModelEncryptor:
    """Class for model encryption."""

    @staticmethod
    def encrypt_folder(folder_path: PathOrStr) -> dict[str, dict]:
        """
        Encrypt folder or ML model recursively.

        Args:
        ----
            folder_path(PathOrStr): Path to the folder to encrypt.

        Returns: Decryption dict of str, dict.

        """
        key, aad = bytes.fromhex(token_hex(16)), bytes.fromhex(token_hex(16))
        logger.info(f"Encrypting model/folder at {folder_path}")
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f'No directory at {folder_path}')

        for root, dirs, files in os.walk(folder_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                with open(file_path, "rb") as f_read:
                    data = f_read.read()
                cipher = AES.new(key, AES.MODE_GCM, nonce=aad)
                encrypted_data, tag = cipher.encrypt_and_digest(
                    pad(data, AES.block_size)
                )
                encrypted_file_path = file_path + ".encrypted"
                with open(encrypted_file_path, "wb") as f_write:
                    f_write.write(b64encode(cipher.nonce + tag + encrypted_data))
                os.remove(file_path)
        logger.debug("Finished model/folder encryption.")
        return {
            'model': {
                'key': key.hex(),
                'aad': aad.hex()
            }
        }

    @staticmethod
    def decrypt_folder(folder_path: PathOrStr, key: str, aad: str) -> None:
        """
        Decrypt folder or ML model recursively.

        Args:
        ----
        folder_path(PathOrStr): Path to the folder to decrypt.
        key(str): The hexadecimal key.
        aad(str): The hexadecimal aad.

        Returns: Decryption dict of str, dict.

        """
        key_bytes, aad_bytes = bytes.fromhex(key), bytes.fromhex(aad)
        logger.info(f"Decrypting model/folder at {folder_path}")
        for root, dirs, files in os.walk(folder_path):
            for filename in files:
                if filename.endswith(".encrypted"):
                    file_path = os.path.join(root, filename)
                    with open(file_path, "rb") as f:
                        encrypted_data = b64decode(f.read())
                        tag = encrypted_data[16:32]
                        ciphertext = encrypted_data[32:]
                    cipher = AES.new(key_bytes, AES.MODE_GCM, nonce=aad_bytes)
                    decrypted_data = unpad(
                        cipher.decrypt_and_verify(ciphertext, tag),
                        AES.block_size
                    )
                    decrypted_file_path = file_path[:-len(".encrypted")]
                    with open(decrypted_file_path, "wb") as f:
                        f.write(decrypted_data)
                    os.remove(file_path)
        logger.debug("Finished model/folder decryption.")

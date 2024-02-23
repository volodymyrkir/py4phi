"""Module that contains constants for the py4phi library."""
import os
# TODO fill consts

DEFAULT_SECRET_NAME = 'secret.key'
DEFAULT_CONFIG_NAME = 'decrypt.conf'

DEFAULT_PY4PHI_ENCRYPTED_NAME = 'py4phi_encrypted_outputs'
DEFAULT_PY4PHI_DECRYPTED_NAME = 'py4phi_decrypted_outputs'
DEFAULT_PY4PHI_ENCRYPTED_PATH = os.getcwd()
DEFAULT_PY4PHI_DECRYPTED_PATH = os.getcwd()

TMP_OUTPUT_DIR = "tmp-spark"

PYSPARK = 'pyspark'
POLARS = 'polars'
PANDAS = 'pandas'

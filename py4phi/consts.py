"""Module that contains constants for the py4phi library."""
import os

DEFAULT_SECRET_NAME = 'secret.key'
DEFAULT_CONFIG_NAME = 'decrypt.conf'

DEFAULT_FEATURE_SELECTION_FOLDER_NAME = 'py4phi_fs_outputs'
DEFAULT_FEATURE_SELECTION_OUTPUTS_NAME = 'py4phi_fs_outputs'
DEFAULT_PCA_REDUCED_FOLDER_NAME = 'py4phi_pca_outputs'
DEFAULT_PCA_OUTPUT_NAME = 'pca_outputs'
DEFAULT_PY4PHI_ENCRYPTED_NAME = 'py4phi_encrypted_outputs'
DEFAULT_PY4PHI_DECRYPTED_NAME = 'py4phi_decrypted_outputs'
CWD = os.getcwd()

DEFAULT_MODEL_KEY_PATH = 'py4phi_model_decryption'

TMP_OUTPUT_DIR = "tmp-spark-py4phi"

PYSPARK = 'pyspark'
POLARS = 'polars'
PANDAS = 'pandas'

STR_TAG_START = 16
STR_TAG_END = 32

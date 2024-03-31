"""This module contains descriptions for CLI commands, parameters."""

PATH_TO_INPUT_DESCRIPTION = 'Path to the input file to be encrypted/decrypted/analyzed.'
PATH_TO_OUTPUT_DESCRIPTION = ('Path to the output directory where py4phi outputs should be saved.'
                              'If not supplied, saves outputs to current working directory.')

PRINT_INTERMEDIATE = ('Whether to print in terminal an intermediate results of'
                      ' encryption/decryption for debugging or monitoring purposes.')

READ_OPTIONS = ('Additional key-value argument to pass to py4phi reader.'
                'These arguments are reader-specific.'
                'For example, you can set -r header True for pyspark reader '
                'to treat first line of a CSV file as header.'
                'You can provide multiple key-value pairs.'
                'Boolean values are supported.'
                'They are not case-sensitive, '
                'i.e. header True and header TrUe both work.')

WRITE_OPTIONS = ('Additional key-value argument to pass to py4phi writer.'
                 'These arguments are writer-specific.'
                 'They work the same as reader options.')

FILE_TYPE_DESCRIPTION = 'Type of input file, by default is derived from the file name.'
SAVE_FILE_TYPE = 'Type of output file, read about currently supported formats in README.'

LOG_LEVEL_DESCRIPTION = 'Logging level, by default is INFO.'

COLUMN_TO_ENCRYPT = ('Column to encrypt. You can pass multiple values to encrypt'
                     ' using -c your_col, -c your_col1')
COLUMN_TO_DECRYPT = ('Column to decrypt. You can pass multiple values to decrypt'
                     ' using -c your_col, -c your_col1')

ENGINE_TYPE = 'Engine type to interact with library. Defaults to pyspark.'

DISABLE_CONFIG_ENCRYPTION = ('Flag option. Defaults to false, but when included, it becomes active.'
                             'In case of encryption, it will disable decryption config`s additional encryption.'
                             'This option is not recommended for production usage as it disables additional'
                             'encryption layer.')

CONFIG_NOT_ENCRYPTED = ('Flag option. When included in command execution, py4phi skips config decryption process.'
                        'Should be used only if dataset has been encrypted with --disable_config_encryption option.')

INPUT_PATH_FOLDER = 'Input path for model/folder.'

INPUT_ENCRYPTED_FOLDER = ('Input path for encrypted model/folder.'
                          'Note that along with this path, there should be decryption config(-s).')

TARGET_FEATURE = 'Target feature of your dataset.'
COLUMNS_TO_IGNORE = 'Columns to be ignored and preserved during PCA'
SAVE_REDUCED = 'If enabled, will save dataset with reduced dimensionality.'
CUM_VAR_THRESHOLD = 'Sets cumulative variance recommendation threshold. Defaults to 0.95'
NUM_COMPONENTS = ('Number of components to use for PCA. '
                  'If provided will be used to analyze'
                  'or analyze and actually reduce dimensionality of the dataset. '
                  'If not provided, a recommended number of components will be used.')
NULLS_MODE = ('Nulls handle mode to be used for PCA null handling in the dataset. '
              'Can be "fill" or "drop", where fill will fill each column with the mean value in the column,'
              ' and drop will drop all rows where column is null. '
              'Recommended modes is "fill" with a combination of data pre-processing, '
              'so that no data is lost by using mean.')

SAVE_REDUCED_FEATURES = 'If enabled, will save dataset with reduced amount of features to the specified directory.'
OVERRIDE_DROP_COLUMN = 'Override columns to be dropped. You can provide multiple values.'
TARGET_COR_THRESHOLD = ('Sets correlation threshold when combining features and the target feature. '
                        'By default, set to 0.5. Can be in range [0.0-1.0]. All features that have'
                        ' correlation with the target feature less than the specified value,'
                        ' will be recommended for dropping.')
FEATURE_COR_THRESHOLD = ('Sets correlation threshold when combining features with each other. '
                         'By default, set to 0.5. Can be in range [0.0-1.0]. All features that have'
                         ' correlation with each other more than the specified value,'
                         ' will be recommended for dropping.')

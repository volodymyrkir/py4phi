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

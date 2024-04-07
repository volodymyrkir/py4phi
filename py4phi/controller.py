"""Module containing main logic and entrypoint for library."""
import os

from typing import Type, Optional

import pandas as pd
import polars as pl
from pyspark.sql import DataFrame

from py4phi.analytics.principal_component_analysis import PrincipalComponentAnalysis
from py4phi.analytics.feature_selection import FeatureSelection
from py4phi._encryption._polars_encryptor import _PolarsEncryptor
from py4phi.config_processor import ConfigProcessor
from py4phi.dataset_handlers.base_dataset_handler import (
    BaseDatasetHandler, DataFrame as BaseDF
)
from py4phi.dataset_handlers.pandas_dataset_handler import PandasDatasetHandler
from py4phi.dataset_handlers.pyspark_dataset_handler import PySparkDatasetHandler
from py4phi.dataset_handlers.polars_dataset_handler import PolarsDatasetHandler
from py4phi._encryption._pyspark_encryptor import _PySparkEncryptor
from py4phi._encryption._pandas_encryptor import _PandasEncryptor, _BaseEncryptor

from py4phi.logger_setup import logger
from py4phi.utils import prepare_location
from py4phi.consts import (
    DEFAULT_CONFIG_NAME, DEFAULT_SECRET_NAME, DEFAULT_PY4PHI_ENCRYPTED_NAME,
    CWD, DEFAULT_PY4PHI_DECRYPTED_NAME, DEFAULT_PCA_REDUCED_FOLDER_NAME,
    DEFAULT_PCA_OUTPUT_NAME, DEFAULT_FEATURE_SELECTION_FOLDER_NAME,
    DEFAULT_FEATURE_SELECTION_OUTPUTS_NAME, PANDAS, PYSPARK, POLARS,
)

ENGINE_NAME_MAPPING = {
    PYSPARK: PySparkDatasetHandler,
    PANDAS: PandasDatasetHandler,
    POLARS: PolarsDatasetHandler
}

HANDLERS_TYPE_MAPPING = {
    DataFrame: PySparkDatasetHandler,
    pd.DataFrame: PandasDatasetHandler,
    pl.DataFrame: PolarsDatasetHandler
}


class Controller:
    """Class to interact with py4phi components."""

    ENCRYPTION_MAPPING: dict[Type[BaseDatasetHandler], Type[_BaseEncryptor]] = {
        PySparkDatasetHandler: _PySparkEncryptor,
        PandasDatasetHandler: _PandasEncryptor,
        PolarsDatasetHandler: _PolarsEncryptor,
    }

    def __init__(self, dataset_handler: BaseDatasetHandler):
        self._dataset_handler = dataset_handler
        self._encryption_cls = self.ENCRYPTION_MAPPING[type(dataset_handler)]
        self.__encryptor = None
        self._encrypted: bool = False
        self._decrypted: bool = False
        self.__columns_data = None
        self._current_df: BaseDF = self._dataset_handler.df
        self._config_processor = ConfigProcessor()

    def print_current_df(self) -> None:
        """Print dataframe of current state."""
        logger.info('Printing current dataframe.')
        self._dataset_handler.print_df(self._current_df)

    def encrypt(
            self,
            columns_to_encrypt: list[str],
    ) -> BaseDF:
        """
        Encrypt specified columns in dataset.

        Args:
        ----
        columns_to_encrypt (list[str]): List of columns to be encrypted.

        Returns: (BaseDF) An encrypted dataframe.

        """
        self.__encryptor = (
            self._encryption_cls(self._current_df, columns_to_encrypt)
            if not self.__encryptor
            else self.__encryptor
        )

        logger.info(f'Kicking off encryption on current dataframe '
                    f'for columns: {columns_to_encrypt}.')
        self._current_df, self.__columns_data = self.__encryptor.encrypt()
        self._encrypted = True
        logger.info('Successfully encrypted dataframe.')
        return self._current_df

    def save_encrypted(
            self,
            output_name: str = 'output_dataset',
            save_location: str = CWD,
            save_format: str = None,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            encrypt_config: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME,
            **kwargs
    ) -> None:
        """
        Save dataframe using provided location and parameters.

        Args:
        ----
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config.
                                            Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        save_format (str, optional): Format of output file. Defaults to None,
            If None - csv will be used.
        config_file_name (str, optional): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str, optional): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        Raises:
        ------
        ValueError: If dataset is being saved without preliminary encryption.

        """
        if not self._encrypted:
            logger.warning('Perhaps you forgot to encrypt your dataframe, aborting.')
            raise ValueError('No encryption action taken!')

        save_location = os.path.join(save_location, DEFAULT_PY4PHI_ENCRYPTED_NAME)
        prepare_location(location=save_location)

        self._config_processor.save_config(
            self.__columns_data,
            path=save_location,
            conf_file_name=config_file_name,
            encrypt_config=encrypt_config,
            key_file_name=key_file_name
        )
        logger.debug(f'Saved config to: {save_location}.')
        self._dataset_handler.write(
            self._current_df,
            output_name,
            save_location,
            save_format,
            **kwargs
        )
        logger.info(f'Saved outputs to: {save_location}.')

    def save_decrypted(
            self,
            output_name: str = 'output_dataset',
            save_location: str = CWD,
            save_format: str = 'csv',
            **kwargs
    ) -> None:
        """
        Save decrypted dataframe. Can be used without.

        Args:
        ----
        output_name (str): Name of te output file.
        encrypt_config (bool, optional): Whether to encrypt config.
                                            Defaults to True.
        save_location (str, optional): Folder location to save all the outputs.
        save_format (str, optional): Format to save file. Defaults to 'csv'.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        """
        save_location = os.path.join(save_location,
                                     DEFAULT_PY4PHI_DECRYPTED_NAME)
        if not self._decrypted:
            logger.warn('No decryption action taken! '
                        'Perhaps you forgot to decrypt your dataframe. '
                        'Saving non-decrypted data.')
        prepare_location(location=save_location)

        self._dataset_handler.write(
            self._current_df,
            output_name,
            save_location,
            save_format=save_format,
            **kwargs
        )
        logger.info(f'Saved outputs to: {save_location}.')

    def decrypt(
            self,
            columns_to_decrypt: list[str],
            configs_path: str = CWD,
            config_file_name: str = DEFAULT_CONFIG_NAME,
            config_encrypted: bool = True,
            key_file_name: str = DEFAULT_SECRET_NAME
    ) -> BaseDF:
        """
        Decrypt specified columns in dataset.

        Args:
        ----
        columns_to_decrypt (list[str]): List of columns to be decrypted.
        configs_path (str, optional): Path to the directory,
                                        containing the decryption configs.
                                        Defaults to current working directory.
        config_encrypted (bool): Whether config is encrypted.
                                    Defaults to True.
        config_file_name (str): Name of config to be saved.
                                    Defaults to DEFAULT_CONFIG_NAME.
        key_file_name (str): Name of config to be saved.
                                Defaults to DEFAULT_SECRET_NAME.

        Returns: Decrypted dataframe.

        """
        self.__encryptor = (
            self._encryption_cls(self._current_df, columns_to_decrypt)
            if not self.__encryptor
            else self.__encryptor
        )
        if self.__columns_data:
            logger.debug(self.__columns_data)
            decryption_dict = {key: val for key, val in self.__columns_data.items()
                               if key in columns_to_decrypt}
            logger.debug("Decrypting previously encrypted dataframe, "
                         f"for columns {list(decryption_dict.keys())} "
                         "ignoring provided configs paths...")
        else:
            logger.info(f'Kicking off decryption on current dataframe on columns: '
                        f'{columns_to_decrypt}. ')
            try:
                logger.info(f'Config path is {configs_path}. '
                            f"{'Config is encrypted' if config_encrypted else ''}")
                decryption_dict = self._config_processor.read_config(
                    configs_path,
                    conf_file_name=config_file_name,
                    config_encrypted=config_encrypted,
                    key_file_name=key_file_name
                )
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"Decryption config files not found under {configs_path}. "
                )

        self._current_df = self.__encryptor.decrypt(decryption_dict)
        self._decrypted = True
        logger.info('Successfully decrypted current df.')
        return self._current_df

    def perform_pca(
            self,
            target_feature: Optional[str] = None,
            ignore_columns: list[str] = None,
            save_reduced: bool = False,
            rec_threshold: Optional[float] = 0.95,
            n_components: Optional[int] = None,
            nulls_mode: str = 'fill',
            save_format: Optional[str] = 'CSV',
            save_folder: str = CWD,
            save_name: str = DEFAULT_PCA_OUTPUT_NAME,
            **kwargs
    ) -> None:
        """
        Perform PCA and reduce dimensionality of the dataset.

        Please take a close look at all the parameters' documentation.

        Args:
        ----
        target_feature (Optional[str]): To keep the target feature, provide its name.
                                            Defaults to None.

        ignore_columns (list[str]): To ignore additional columns (e.g. string columns),
                                        provide them to this parameter.
                                        This will preserve columns,
                                        i.e. they will not be used in PCA.
                                        Defaults to None.

        save_reduced (bool): By default is False.
            If True, it will reduce dimensionality and save the reduced dataset
            to the specified folder.
            By default, only performs analysis and suggests how many components to save.

        rec_threshold (Optional[float]): Sets recommendation
                                            cumulative variance threshold.
                                            Defaults to 0.95.

        n_components (Optional[int]): Number of components
                                        to use for analysis and reduction.
                                        It is not recommended to set this parameter,
                                        by default all features are used for analysis,
                                        and only recommended amount
                                         - for actual reduction. Defaults None.

        nulls_mode (str): The mode to be used in the fill_or_drop_nulls function.
                          By default, will use 'fill' to fill missing values with means.
                          Available option is 'drop' to drop rows with missing values.

        save_format (Optional[str]): Format to use for saving the reduced dataset.
                                        Defaults to CSV file type.
        save_folder (Optional[str]): Path to save the reduced dataset.
                                        Defaults to current working directory.
        save_name (Optional[str]): Name of the file to save the reduced dataset.
                                        Defaults to 'pca_outputs'.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        """
        df = self._dataset_handler.to_pandas()
        analyzer = PrincipalComponentAnalysis(
            df,
            target_feature
        )
        result = analyzer.component_analysis(
            ignore_columns=ignore_columns,
            rec_threshold=rec_threshold,
            n_components=n_components,
            reduce_features=save_reduced,
            nulls_mode=nulls_mode
        )

        if save_reduced:
            logger.info('Finished PCA reduction.'
                        f' Output dataset has {len(result.columns)} features total.')
            handler = PandasDatasetHandler()
            save_location = os.path.join(save_folder, DEFAULT_PCA_REDUCED_FOLDER_NAME)
            prepare_location(location=save_location)
            logger.info(f'Saving reduced dataframe to {save_location}')
            handler.write(
                result,
                name=save_name,
                path=save_location,
                save_format=save_format,
                **kwargs
            )

    def perform_feature_selection(
            self,
            target_feature: str,
            drop_recommended: bool = False,
            override_columns_to_drop: list[str] = None,
            target_correlation_threshold: Optional[float] = 0.5,
            features_correlation_threshold: Optional[float] = 0.5,
            save_format: Optional[str] = 'CSV',
            save_folder: str = CWD,
            save_name: str = DEFAULT_FEATURE_SELECTION_OUTPUTS_NAME,
            **kwargs
    ) -> None:
        """
        Perform feature selection.

        Requires target feature to be passed.
        Suggests columns to be dropped based on correlational analysis.

        Please take a close look at all the parameters' documentation.

        Args:
        ----
        target_feature (str): The target feature to be used for correlational analysis.
        drop_recommended (bool): Whether to ONLY find potentially redundant columns.
                                    If set to True, will drop them from the dataset.
                                    Defaults to False.

        override_columns_to_drop (list[str]): List of columns to be dropped.
            If provided, no recommendations are taken into consideration,
            i.e. only dropping specified columns. Defaults to None.

        target_correlation_threshold (Optional[float]): Sets correlation threshold
            while recommending features to be dropped based on correlational analysis
            against the target feature. Can be 0.0-1.0. Defaults to 0.5.

        features_correlation_threshold (Optional[float]): Sets correlation threshold
            while recommending features to be dropped based on correlational analysis
            against each other. Can be 0.0-1.0. Defaults to 0.5.

        save_format (Optional[str]): Format to use for saving the reduced dataset.
                                        Defaults to CSV file type.
        save_folder (Optional[str]): Name of the folder to save the reduced dataset.
                                        Defaults to 'py4phi_pca_outputs'.
        save_name (Optional[str]): Name of the file to save the reduced dataset.
                                        Defaults to 'pca_outputs'.
        kwargs (dict, optional): keyword arguments to be supplied to dataframe writing.

        """
        df = self._dataset_handler.to_pandas()

        analyzer = FeatureSelection(
            df,
            target_feature
        )
        result = analyzer.correlation_analysis(
            override_columns_to_drop=override_columns_to_drop,
            drop_recommended=drop_recommended,
            target_corr_threshold=target_correlation_threshold,
            feature_corr_threshold=features_correlation_threshold,
        )

        if drop_recommended:
            logger.info('Finished feature selection process. '
                        f'Output dataset has {len(result.columns)} features in total.')
            handler = PandasDatasetHandler()
            save_location = os.path.join(
                save_folder, DEFAULT_FEATURE_SELECTION_FOLDER_NAME
            )
            prepare_location(location=save_location)
            logger.info(f'Saving reduced dataframe to {save_location}')
            handler.write(
                result,
                name=save_name,
                path=save_location,
                save_format=save_format,
                **kwargs
            )

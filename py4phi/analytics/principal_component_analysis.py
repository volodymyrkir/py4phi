"""Module with principal component analysis logic for py4phi."""
import numpy as np
from numpy.typing import NDArray
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from py4phi.logger_setup import logger
from py4phi.analytics.base_analytics import Analytics


class PrincipalComponentAnalysis(Analytics):
    """Class to perform principal component analysis on a pandas dataframe."""

    def __init__(self, df: pd.DataFrame, target_column: str | None = None) -> None:
        super().__init__(df, target_column)
        self.scaler = StandardScaler()

    @staticmethod
    def _find_recommended_components(
            explained_variance: NDArray,
            rec_threshold: float
    ) -> int:
        """
        Find the recommended number of PCA components based on explained variance.

        Args:
        ----
        explained_variance (NDArray): A NumPy array containing the
         explained variance ratio per component.
        rec_threshold (float): The desired minimum explained variance ratio.

        Returns: (int) The recommended number of PCA components.
        """
        cumulative_variance = 0
        num_components = 0

        for var in explained_variance:
            cumulative_variance += var
            num_components += 1
            if cumulative_variance >= rec_threshold:
                break

        if cumulative_variance < rec_threshold:
            num_components = len(explained_variance)

        return num_components

    def handle_nulls(self, df: pd.DataFrame, mode: str = 'fill') -> pd.DataFrame:
        """
        Remove null values from a pandas dataframe.

        Nulls are handled by either filling them with mean,
        or simply removing rows with NAs in any of the columns.
        If target_column is provided, it is used to fill missing
         values with mean value for the particular class of the target feature.

        Args:
        ----
        df (pd.DataFrame): Input pandas dataframe.
        mode (str): The mode to be used while handling NA values.
                    By default, will use 'fill' to fill missing values with means.
                    The alternative option is 'drop' to drop rows with missing values.

        Returns: (pd.DataFrame) A dataframe without NA values.
        """
        if mode == 'fill':
            for column in [col for col in df.columns if col != self._target_column]:
                if self._target_column:
                    logger.info(f'Filling empty values for column {column} '
                                'with mean based on target feature.')
                    df[column] = self._df[column].fillna(
                        self._df.groupby(self._target_column)[column].transform('mean')
                    )
                else:
                    logger.info(f'Filling empty values for column {column} '
                                'with mean of the column.')
                    df[column] = self._df[column].fillna(self._df[column].mean())
        elif mode == 'drop':
            len_before = len(df)
            df = df.dropna(axis=0, how='any')
            logger.warning(f'Dropped {len_before - len(df)} records with NA values.')
        else:
            raise ValueError(f'Not supported null handle mode {mode}.')

        return df

    def component_analysis(
            self,
            ignore_columns: list[str] = None,
            rec_threshold: float = 0.95,
            n_components: int | None = None,
            reduce_features: bool = False,
            nulls_mode: str = 'fill'
    ) -> pd.DataFrame | None:
        """
        Perform PCA on a dataset and suggest number of components to be preserved.

        PCA is a dimension reduction technique,
        read more at https://scikit-learn.org/stable/modules/generated/sklearn.decomposition.PCA.html

        Args:
        ----
        ignore_columns (list[str]): List of columns to be ignored during PCA.
        rec_threshold (float): Explained variance threshold
                                for PC number recommendation.
        n_components (int): The number of principal components to analyze with.
                      Defaults to 95%.
        reduce_features (bool): Whether to actually perform PCA and reduce dataset.
        nulls_mode (str): The mode to be used in the fill_or_drop_nulls function.
                          By default, will use 'fill' to fill missing values with means.
                          Available option is 'drop' to drop rows with missing values.

        Returns:
        -------
        pd.DataFrame | None: If reduce_features is provided, returns reduced dataframe,
                                otherwise just logs recommendations.

        Raises:
        ------
        TypeError: If the input df contains non-numeric columns.
        ValueError: If n_components parameter is provided,
                        and it is more than current number of features.
                        OR
                    If provided dataframe is empty, considering all columns,
                        except for target and ignored columns.
        """
        features = self._df
        columns_to_drop = [self._target_column] if self._target_column else []
        if ignore_columns:
            for column in ignore_columns:
                columns_to_drop.append(column)

        if columns_to_drop:
            features = features.drop(columns_to_drop, axis=1)
        try:
            for col in features.columns:
                features[col] = pd.to_numeric(features[col])
        except ValueError:
            raise TypeError(
                f"Provided dataset contains non-numeric column {col}. "
                "Consider using other techniques for dimensionality reduction"
                "or pass this column to the ignore_columns parameter."
            )

        features = self.handle_nulls(features, nulls_mode)

        if len(features.columns) == 0:
            raise ValueError("Provided dataset is empty. Cannot perform PCA.")

        scaled_data = self.scaler.fit_transform(features)
        pca_components = n_components or features.shape[1]
        if pca_components > features.shape[1]:
            raise ValueError("Can't perform PCA with number of "
                             "components more than the number of original features.")
        logger.info(f'Performing principal component analysis'
                    f' with {pca_components} components.')
        pca = PCA(n_components=pca_components)

        pca.fit(scaled_data)
        explained_variance = pca.explained_variance_ratio_
        logger.info('PCA explained variance:')
        for component, variance in enumerate(explained_variance, 1):
            logger.info(f'Component #{component}: {variance:.2%}')

        n_components_to_keep = self._find_recommended_components(
            explained_variance, rec_threshold
        )

        logger.info("Explained variance suggests keeping"
                    f" at least {n_components_to_keep} components "
                    f"({rec_threshold:.2%} cumulative variance).")

        if reduce_features:
            if n_components:
                final_components = n_components
                logger.info(f"Using {n_components} components "
                            "as provided by User.")
            else:
                final_components = n_components_to_keep
                logger.info(f"Using {n_components_to_keep} components "
                            "automatically as recommended above.")

            return self._reduce_features(
                final_components,
                scaled_data,
                columns_to_drop
            )

    def _reduce_features(
            self,
            n_components_to_keep: int,
            scaled_data: np.ndarray,
            columns_to_join: list[str]
    ) -> pd.DataFrame:
        """
        Actually performs PCA and reduces dataset's dimensionality.

        Args:
        ----
        n_components_to_keep (list[str]): Number of PCA components to keep.
        scaled_data (np.ndarray): Scaled data from previous step.
        columns_to_join (list[str]): List of columns to join back to dataset.

        Returns:
        -------
        pd.DataFrame: reduced dataframe

        """
        logger.info(f'Reducing features to {n_components_to_keep}')
        pca = PCA(n_components=n_components_to_keep)

        pca.fit(scaled_data)
        principal_components = pca.transform(scaled_data)

        reduced_data = pd.DataFrame(
            principal_components,
            columns=[f"PC{i + 1}" for i in range(n_components_to_keep)]
        )
        if columns_to_join:
            dropped_df = self._df[columns_to_join]
            reduced_data = pd.concat([reduced_data, dropped_df], axis=1)
        return reduced_data

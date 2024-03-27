"""Module with principal component analysis logic for py4phi."""
import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from py4phi.logger_setup import logger


class PrincipalComponentAnalysis:
    """Class to perform principal component analysis on a pandas dataframe."""

    def __init__(self, df: pd.DataFrame, target_column: str | None = None) -> None:
        self._df = df
        self._target_column = target_column
        self.scaler = StandardScaler()

    def component_analysis(
            self,
            ignore_columns: list[str] = None,
            rec_threshold: float = 0.95,
            n_components: int | None = None,
            reduce_features: bool = False
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

        Returns:
        -------
        pd.DataFrame | None: If reduce_features is provided, returns reduced dataframe,
                                otherwise just logs recommendations.

        Raises:
        ------
        TypeError: If the input df contains non-numeric columns.
        ValueError: If n_components parameter is provided,
                        and it is more than current number of features.

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
            raise TypeError(f"Dataset contains non-numeric column {col}. "
                            "Consider using MFA for dimensionality reduction."
                            "Or pass this column to the ignore_columns argument")

        scaled_data = self.scaler.fit_transform(features)
        pca_components = n_components or features.shape[1]
        if pca_components > features.shape[1]:
            raise ValueError("Can't perform PCA with number of "
                             "components more than number of original features.")
        logger.info(f'Performing analysis-PCA with {pca_components} components.')
        pca = PCA(n_components=pca_components)

        pca.fit(scaled_data)
        explained_variance = pca.explained_variance_ratio_
        logger.info('PCA explained variance:')
        for feature, variance in zip(features.columns, explained_variance):
            logger.info(f'Feature {feature}: {variance:.2f}')
        n_components_to_keep = np.where(
            np.cumsum(pca.explained_variance_ratio_) >= rec_threshold
        )[0][0] + 1
        logger.info("Explained variance suggests keeping"
                    f" at least {n_components_to_keep} components "
                    f"({rec_threshold:%} cumulative variance).")

        if reduce_features:
            if n_components:
                final_components = n_components
                logger.info(f"Using {n_components} components "
                            f"as provided by User.")
            else:
                final_components = n_components_to_keep
                logger.info(f"Using {n_components_to_keep} components "
                            f"automatically as recommended above.")

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


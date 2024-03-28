"""Module with feature selection logic for py4phi."""
import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency

from py4phi.logger_setup import logger
from py4phi.analytics.base_analytics import Analytics


class FeatureSelection(Analytics):
    """Class to perform feature selection on a pandas dataframe."""

    @staticmethod
    def cramers_v(confusion_matrix: pd.DataFrame) -> float:
        """
        Calculate Cramer's V, a measure of association for two categorical variables.

        Args:
        ----
        confusion_matrix (pd.DataFrame): A confusion matrix
            representing the contingency table between two categorical variables.

        Returns: (float) The Cramer's V statistic, ranging
                    from 0 (no association) to 1 (strong association).
        """
        chi2 = chi2_contingency(confusion_matrix)[0]
        n = confusion_matrix.sum().sum()
        phi2 = chi2 / n
        r, k = confusion_matrix.shape
        phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
        rcorr = r - ((r - 1) ** 2) / (n - 1)
        kcorr = k - ((k - 1) ** 2) / (n - 1)
        return np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1)))

    def correlation_analysis(
        self,
        target_corr_threshold,
        feature_corr_threshold,
        drop_recommended: bool,
    ) -> pd.DataFrame:
        """
        Perform correlation analysis using given target feature.

        This algorithm finds the Pearson correlation
         between features related to target feature, and between each other.
          Then, based on the correlation thresholds, features may be
          recommended for dropping.
          For measuring categorical correlation, Cramér's V measure is used.


        Args:
        ----
        ignore_columns (list[str]): List of columns to be ignored during PCA.

        target_corr_threshold (Optional[float]): Sets correlation threshold
            while recommending features to be dropped based on correlational analysis
            against the target feature. Can be 0.0-1.0

        feature_corr_threshold (Optional[float]): Sets correlation threshold
            while recommending features to be dropped based on correlational analysis
            against each other. Can be 0.0-1.0.
        drop_recommended (bool): Whether to follow recommendations and drop features.

        Returns: (pd.DataFrame) The dataframe with
         either dropped features or original dataframe.

        """
        df = self._df
        correlations = {}
        for col in df.columns:
            if col != self._target_column:
                if pd.api.types.is_numeric_dtype(df[col]):
                    correlation = df[col].corr(df[self._target_column])
                else:
                    confusion_matrix = pd.crosstab(df[col], df[self._target_column])
                    correlation = self.cramers_v(confusion_matrix)
                correlations[col] = correlation

        logger.info("Correlations with the target feature:")
        for col, corr in correlations.items():
            logger.info(f'Feature {col}: {corr}')
        low_correlation_features = [
            col
            for col, corr in correlations.items()
            if abs(corr) < target_corr_threshold
        ]

        logger.info(f"Features with low correlation "
                    f"with the target feature:{low_correlation_features}")

        if drop_recommended:
            logger.info("Dropping low correlation features listed above.")
            df = df.drop(low_correlation_features, axis=1)

        correlation_matrix_numeric = df.select_dtypes(
            include=['float64', 'int64']
        ).corr().abs()
        correlation_matrix_categorical = df.select_dtypes(include='object').apply(
            lambda x: x.astype('category').cat.codes
        ).corr().abs()

        categorical_features_to_drop = self.find_correlated_pairs(
            correlation_matrix_categorical, feature_corr_threshold
        )
        numeric_features_to_drop = self.find_correlated_pairs(
            correlation_matrix_numeric, feature_corr_threshold
        )

        features_to_drop = numeric_features_to_drop.union(
            categorical_features_to_drop
        )
        logger.info(f"Features to drop (keeping one from each pair): "
                    f"{features_to_drop}")

        if drop_recommended:
            df = df.drop(features_to_drop, axis=1)
        return df

    @staticmethod
    def find_correlated_pairs(
            correlation_matrix,
            features_corr_threshold: float
    ) -> set[str]:
        """
        Given correlation matrix, find highly correlated features.

        Args:
        ----
        correlation_matrix (#TODO): Correlation matrix of features.
        features_corr_threshold (Optional[float]): Sets correlation threshold
            while finding highly correlated features.

        Returns: (set[str]) The set of features found.

        """
        highly_correlated_indices = np.where(
            correlation_matrix > features_corr_threshold
        )
        highly_correlated_pairs = [
            (correlation_matrix.index[i],
             correlation_matrix.columns[j])
            for i, j in zip(*highly_correlated_indices) if i < j
        ]
        features_to_drop = set()
        for feature1, feature2 in highly_correlated_pairs:
            if feature1 not in features_to_drop:
                features_to_drop.add(feature2)
        logger.info("Highly correlated numeric features:")
        for feature1, feature2 in highly_correlated_pairs:
            logger.info(f"'{feature1}' is highly correlated with '{feature2}'.")
        return features_to_drop




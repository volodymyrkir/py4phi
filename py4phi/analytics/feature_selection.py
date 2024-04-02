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
            target_corr_threshold: float,
            feature_corr_threshold: float,
            override_columns_to_drop: list[str],
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

        override_columns_to_drop (list[str]): List of columns to be dropped.
            If provided, no recommendations are taken into consideration,
            i.e. only dropping specified columns.

        drop_recommended (bool): Whether to follow recommendations and drop features.

        Returns: (pd.DataFrame) The dataframe with
         either dropped features or original dataframe.

        """
        df = self._df.drop(self._target_column, axis=1)
        target = self._df[self._target_column].copy()
        correlations = {}
        if not pd.api.types.is_numeric_dtype(target):
            target = target.astype('category').cat.codes

        for col in df.columns:
            if col != self._target_column:
                if pd.api.types.is_numeric_dtype(df[col]):
                    correlation = df[col].corr(target)
                else:
                    confusion_matrix = pd.crosstab(df[col], target)
                    correlation = self.cramers_v(confusion_matrix)
                correlations[col] = correlation if not pd.isna(correlation) else .0

        logger.info("Correlations with the target feature:")
        for col, corr in correlations.items():
            logger.info(f'Feature {col}: {corr}')
        dropping_recommendations = [
            col
            for col, corr in correlations.items()
            if abs(corr) < target_corr_threshold
        ]

        logger.info(f"Features with low correlation "
                    f"with the target feature:{dropping_recommendations}")
        correlation_matrix_numeric = df.select_dtypes(
            include=['float64', 'int64']
        ).corr().abs()

        correlation_matrix_categorical = (
            df.select_dtypes(include='object').apply(
                lambda x: x.astype('category').cat.codes
            ).corr().abs()
        )
        logger.info("Finding highly correlated features...")
        dropping_recommendations = self.find_correlated_pairs(
            dropping_recommendations,
            correlation_matrix_categorical,
            feature_corr_threshold
        )
        dropping_recommendations = self.find_correlated_pairs(
            dropping_recommendations,
            correlation_matrix_numeric,
            feature_corr_threshold
        )

        logger.info(f"Total recommended features to drop: "
                    f"{dropping_recommendations}")
        features_left = set(self._df.columns).difference(dropping_recommendations)
        logger.info(f"Features that will be left after dropping: "
                    f"{features_left}")
        if override_columns_to_drop:
            logger.info(f"Ignoring recommendations "
                        f"and dropping following columns: {override_columns_to_drop}.")
            return self._df.drop(list(override_columns_to_drop), axis=1)
        elif drop_recommended:
            return self._df.drop(dropping_recommendations, axis=1)

    @staticmethod
    def find_correlated_pairs(
            current_recommendations: list[str],
            correlation_matrix: pd.DataFrame,
            features_corr_threshold: float
    ) -> list[str]:
        """
        Find highly correlated features.

        Given correlation matrix, find and add them to the dropping recommendation list.

        Args:
        ----
        current_recommendations (list[str]): List of features
                                                to be recommended for dropping.
        correlation_matrix (pd.DataFrame): Correlation matrix of features.
        features_corr_threshold (Optional[float]): Sets correlation threshold
            while finding highly correlated features.

        Returns: (list[str]) The modified recommendations list.

        """
        highly_correlated_indices = np.where(
            correlation_matrix > features_corr_threshold
        )
        highly_correlated_pairs = [
            (correlation_matrix.index[i],
             correlation_matrix.columns[j],
             correlation_matrix.iloc[i, j])
            for i, j in zip(*highly_correlated_indices) if i < j
        ]

        for feature1, feature2, corr_value in highly_correlated_pairs:
            if (feature1 not in current_recommendations
                    and feature2 not in current_recommendations):
                current_recommendations.append(feature2)
                logger.info(
                    f"'{feature1}' is highly correlated with '{feature2}' "
                    f"(correlation: {corr_value:.4f})."
                    f"\nAdding {feature2} to the drop recommendations."
                    f"You can provide {feature1} to the 'override_columns_to_drop' "
                    "parameter to override recommendations.")
        return current_recommendations

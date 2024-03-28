"""Module with principal component analysis logic for py4phi."""
from abc import ABC

import pandas as pd


class Analytics(ABC):
    """
    Base class for analytics.

    Initializes pandas dataframe field and target column.
    """

    def __init__(self, df: pd.DataFrame, target_column: str | None = None) -> None:
        self._df = df
        self._target_column = target_column

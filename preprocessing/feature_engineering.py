"""Feature engineering and normalization routines for AIS-RF fusion."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


def normalize_numeric_features(df: pd.DataFrame, feature_ranges: Dict[str, tuple[float, float]]) -> pd.DataFrame:
    """Scale numeric features into standardized ranges."""
    # TODO: Apply min-max scaling or z-score normalization per feature.
    logger.debug("Normalizing numeric features with ranges: %s", feature_ranges)
    return df


def encode_categorical_features(df: pd.DataFrame, categorical_columns: list[str]) -> pd.DataFrame:
    """Convert categorical columns into model-consumable encodings."""
    # TODO: Implement one-hot encoding or ordinal mapping as required.
    logger.debug("Encoding categorical columns: %s", categorical_columns)
    return df


def compute_motion_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive additional kinematic features (acceleration, turn rate, etc.)."""
    # TODO: Calculate delta-based features from sog/cog and temporal spacing.
    logger.debug("Computing motion features for DataFrame of size %s", len(df))
    return df

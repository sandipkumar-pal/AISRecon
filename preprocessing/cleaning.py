"""Data cleaning utilities for AIS and RF datasets."""
from __future__ import annotations

from typing import Any

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


def normalize_timestamps(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Ensure timestamps are timezone-aware UTC."""
    # TODO: Implement timestamp normalization with pandas to_datetime + tz conversion.
    logger.debug("Normalizing timestamps for column %s", column)
    return df


def filter_invalid_positions(df: pd.DataFrame, lat_col: str, lon_col: str) -> pd.DataFrame:
    """Remove impossible latitude/longitude combinations."""
    # TODO: Filter out latitudes outside [-90, 90] and longitudes outside [-180, 180].
    logger.debug("Filtering invalid positions using columns %s/%s", lat_col, lon_col)
    return df


def deduplicate_records(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Drop duplicate observations while preserving the latest entries."""
    # TODO: Use DataFrame.drop_duplicates with keep='last'.
    logger.debug("Deduplicating records on subset %s", subset)
    return df


def apply_domain_rules(df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Placeholder for additional maritime domain-specific cleaning rules."""
    # TODO: Implement nav_status and sog/cog plausibility checks.
    logger.debug("Applying domain-specific cleaning rules: %s", kwargs)
    return df

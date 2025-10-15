"""Common scoring utilities for AIS-RF fusion algorithms."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


def compute_candidate_scores(df: pd.DataFrame, weight_config: Dict[str, float]) -> pd.DataFrame:
    """Compute composite scores for candidate RF detections."""
    # TODO: Combine normalized feature weights to produce a scalar score.
    logger.debug("Computing candidate scores with weights: %s", weight_config)
    return df


def apply_threshold(df: pd.DataFrame, score_column: str, threshold: float) -> pd.DataFrame:
    """Filter DataFrame rows by a minimum score threshold."""
    # TODO: Filter DataFrame where score_column >= threshold.
    logger.debug("Applying score threshold %.3f on column %s", threshold, score_column)
    return df

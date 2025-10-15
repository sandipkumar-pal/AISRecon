"""Entity resolution between AIS tracks and RF detections."""
from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


def resolve_entities(
    ais_df: pd.DataFrame,
    rf_df: pd.DataFrame,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Produce resolved entity links between AIS MMSI and RF detections."""
    # TODO: Implement probabilistic matching using spatial-temporal constraints.
    logger.info("Resolving entities for %s AIS and %s RF records", len(ais_df), len(rf_df))
    # TODO: Return a DataFrame with resolved identifiers and confidence scores.
    return pd.DataFrame()

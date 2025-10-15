"""AIS gap filling using RF detections."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class GapFillConfig:
    """Configuration for gap fill algorithm."""

    gap_threshold_minutes: int
    candidate_radius_km: float
    viterbi_transition_penalty: float
    min_candidate_score: float


def detect_gaps(ais_df: pd.DataFrame, config: GapFillConfig) -> pd.DataFrame:
    """Identify AIS track gaps exceeding the configured threshold."""
    # TODO: Compute time deltas per MMSI and flag segments > gap_threshold_minutes.
    logger.debug("Detecting AIS gaps using config: %s", config)
    return pd.DataFrame()


def score_candidates(
    gap_segments: pd.DataFrame,
    rf_df: pd.DataFrame,
    config: GapFillConfig,
) -> pd.DataFrame:
    """Score RF detections as candidate fills for AIS gaps."""
    # TODO: Implement scoring combining spatial, temporal, and signal strength metrics.
    logger.debug("Scoring RF candidates for %s gaps", len(gap_segments))
    return pd.DataFrame()


def viterbi_reconstruct(
    scored_candidates: pd.DataFrame,
    config: GapFillConfig,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Apply Viterbi decoding to build most probable AIS path through RF points."""
    # TODO: Construct state transition graph and decode optimal path.
    logger.debug("Running Viterbi reconstruction with %s candidates", len(scored_candidates))
    return pd.DataFrame(), {}


def merge_gap_fill_results(
    ais_df: pd.DataFrame,
    viterbi_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Merge reconstructed trajectories back into the AIS stream."""
    # TODO: Union original AIS records with Viterbi-generated gap segments.
    logger.debug("Merging gap fill results with original AIS data")
    return ais_df

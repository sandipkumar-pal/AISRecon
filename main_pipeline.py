"""Main orchestrator for AIS-RF fusion pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from algorithms.gap_fill import (
    GapFillConfig,
    detect_gaps,
    merge_gap_fill_results,
    score_candidates,
    viterbi_reconstruct,
)
from algorithms.spoof_detection import (
    SpoofDetectionConfig,
    compute_residuals,
    correct_spoofed_tracks,
    score_spoofing,
)
from entity_resolution.resolver import resolve_entities
from entity_resolution.spatiotemporal_index import build_spatiotemporal_index
from ingest.ais_ingest import AISIngestor
from ingest.rf_ingest import RFIngestor
from output.audit import AuditConfig, record_audit_event
from output.writer import OutputConfig, write_output
from preprocessing.cleaning import (
    apply_domain_rules,
    deduplicate_records,
    filter_invalid_positions,
    normalize_timestamps,
)
from preprocessing.feature_engineering import (
    compute_motion_features,
    encode_categorical_features,
    normalize_numeric_features,
)
from schemas.config import PipelineConfig
from schemas.output import map_to_n6_schema
from utils.config import load_config
from utils.io import DataSourceConfig
from utils.logging import get_logger


logger = get_logger(__name__)


def run_pipeline(config_path: str | Path) -> Dict[str, str]:
    """Execute the AIS-RF fusion pipeline."""
    config: PipelineConfig = load_config(config_path)

    ais_source = DataSourceConfig(**config.ais_source.dict())
    rf_source = DataSourceConfig(**config.rf_source.dict())

    ais_df = AISIngestor(ais_source).load()
    rf_df = RFIngestor(rf_source).load()

    ais_df = normalize_timestamps(ais_df, "ts_utc")
    ais_df = filter_invalid_positions(ais_df, "lat", "lon")
    ais_df = deduplicate_records(ais_df, ["mmsi", "ts_utc"])
    ais_df = apply_domain_rules(ais_df)

    rf_df = normalize_timestamps(rf_df, "ts_utc")
    rf_df = filter_invalid_positions(rf_df, "est_lat", "est_lon")
    rf_df = deduplicate_records(rf_df, ["rf_id", "ts_utc"])

    ais_df = compute_motion_features(
        encode_categorical_features(normalize_numeric_features(ais_df, {}), [])
    )
    rf_df = compute_motion_features(rf_df)

    indexed_rf_df = build_spatiotemporal_index(rf_df, config.indexing)
    resolved_entities = resolve_entities(ais_df, indexed_rf_df, config.indexing)
    logger.info("Resolved %s entity links", len(resolved_entities))

    gap_fill_config = GapFillConfig(**config.gap_fill)
    gaps = detect_gaps(ais_df, gap_fill_config)
    candidate_scores = score_candidates(gaps, rf_df, gap_fill_config)
    viterbi_paths, gap_metrics = viterbi_reconstruct(candidate_scores, gap_fill_config)
    fused_gap_df = merge_gap_fill_results(ais_df, viterbi_paths)

    spoof_config = SpoofDetectionConfig(**config.spoof_detection)
    residuals = compute_residuals(fused_gap_df, rf_df)
    spoof_scores = score_spoofing(residuals, spoof_config)
    corrected_df, spoof_metrics = correct_spoofed_tracks(
        fused_gap_df, spoof_scores, rf_df, spoof_config
    )

    n6_ready = map_to_n6_schema(corrected_df)

    output_cfg = OutputConfig(**config.output)
    output_artifacts = write_output(pd.DataFrame(n6_ready), output_cfg)  # type: ignore[arg-type]

    audit_cfg = AuditConfig(**config.audit)
    record_audit_event(
        {
            "event": "pipeline_completed",
            "gap_metrics": gap_metrics,
            "spoof_metrics": spoof_metrics,
            "records_written": len(n6_ready),
        },
        audit_cfg,
    )

    return output_artifacts


if __name__ == "__main__":
    # TODO: Parse CLI arguments for config path and runtime overrides.
    artifacts = run_pipeline("config.yaml")
    logger.info("Pipeline finished with artifacts: %s", artifacts)

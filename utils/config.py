"""YAML configuration loader for the pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from schemas.config import PipelineConfig
from utils.logging import get_logger


logger = get_logger(__name__)


def load_config(path: str | Path) -> PipelineConfig:
    """Load pipeline configuration from YAML."""
    logger.info("Loading configuration from %s", path)
    with open(path, "r", encoding="utf-8") as f:
        raw_config: Dict[str, Any] = yaml.safe_load(f)
    pipeline_cfg = raw_config.get("pipeline", raw_config)
    # TODO: Apply environment variable overrides if required.
    return PipelineConfig(**pipeline_cfg)

"""Output writer for fused AIS-RF datasets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class OutputConfig:
    """Configuration for writing fused outputs."""

    directory: str
    format: str = "parquet"
    partition_cols: Optional[list[str]] = None
    manifest_name: str = "manifest.json"


def write_output(df: pd.DataFrame, config: OutputConfig) -> Dict[str, Any]:
    """Persist fused output dataset to storage."""
    target_dir = Path(config.directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    if config.format == "parquet":
        output_path = target_dir / "fused_output.parquet"
        logger.info("Writing Parquet output to %s", output_path)
        # TODO: Add partitioned writes using pyarrow or fastparquet.
        df.to_parquet(output_path, index=False)
    else:
        raise ValueError(f"Unsupported output format: {config.format}")
    manifest = _write_manifest(df, target_dir / config.manifest_name)
    return {"output_path": str(output_path), "manifest_path": manifest}


def _write_manifest(df: pd.DataFrame, path: Path) -> str:
    """Write a manifest summarizing output dataset metadata."""
    manifest_data = {
        "record_count": len(df),
        "columns": df.columns.tolist(),
        # TODO: Include schema hashes, data lineage references, etc.
    }
    path.write_text(json_dumps(manifest_data))
    logger.debug("Wrote manifest to %s", path)
    return str(path)


def json_dumps(payload: Dict[str, Any]) -> str:
    """Serialize dictionary to JSON string."""
    # TODO: Replace with orjson if performance requirements demand.
    import json

    return json.dumps(payload, indent=2)

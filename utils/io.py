"""I/O utilities for reading from local or S3-backed storage."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

try:
    import s3fs  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    s3fs = None

from utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class DataSourceConfig:
    """Runtime configuration for a data source."""

    type: str  # "local" or "s3"
    path: Optional[str] = None
    format: str = "parquet"
    options: Optional[Dict[str, Any]] = None
    s3: Optional[Dict[str, Any]] = None


def read_dataframe(config: DataSourceConfig, columns: Optional[list[str]] = None) -> pd.DataFrame:
    """Generic DataFrame reader supporting CSV and Parquet from local or S3."""
    logger.info("Reading %s data from %s", config.format, config.type)
    read_options = config.options or {}
    if config.type == "local":
        if not config.path:
            raise ValueError("Local data source requires 'path'.")
        return _read_local(config.path, config.format, columns, read_options)
    if config.type == "s3":
        return _read_s3(config, columns, read_options)
    raise ValueError(f"Unsupported data source type: {config.type}")


def _read_local(path: str, fmt: str, columns: Optional[list[str]], options: Dict[str, Any]) -> pd.DataFrame:
    """Read DataFrame from local filesystem."""
    if fmt == "parquet":
        return pd.read_parquet(path, columns=columns, **options)
    if fmt == "csv":
        return pd.read_csv(path, usecols=columns, **options)
    raise ValueError(f"Unsupported local format: {fmt}")


def _read_s3(config: DataSourceConfig, columns: Optional[list[str]], options: Dict[str, Any]) -> pd.DataFrame:
    """Read DataFrame from S3 using s3fs."""
    if s3fs is None:
        raise ImportError("s3fs is required for S3 data sources.")
    if not config.s3:
        raise ValueError("S3 data source requires 's3' configuration block.")
    bucket = config.s3.get("bucket")
    key = config.s3.get("key")
    profile = config.s3.get("profile")
    if not bucket or not key:
        raise ValueError("S3 configuration must include 'bucket' and 'key'.")
    fs = s3fs.S3FileSystem(profile=profile) if profile else s3fs.S3FileSystem()
    s3_path = f"{bucket}/{key}"
    logger.debug("Reading from S3 path %s", s3_path)
    with fs.open(s3_path) as f:
        if config.format == "parquet":
            return pd.read_parquet(f, columns=columns, **options)
        if config.format == "csv":
            return pd.read_csv(f, usecols=columns, **options)
        raise ValueError(f"Unsupported S3 format: {config.format}")

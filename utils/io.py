"""I/O utilities for reading from local or S3-backed storage."""
from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
import io
from typing import Any, Dict, Optional
import zipfile

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
    """Generic DataFrame reader supporting CSV, Parquet, and zipped payloads."""
    logger.info("Reading %s data from %s", config.format, config.type)
    read_options = dict(config.options or {})
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
    if fmt == "zip":
        return _read_zip_archive(path, columns, options)
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
    s3_path = f"{bucket}/{key}"
    logger.debug("Reading from S3 path %s", s3_path)
    storage_options = options.pop("storage_options", {})
    if profile:
        storage_options.setdefault("profile", profile)
    fs = s3fs.S3FileSystem(**storage_options)
    with fs.open(s3_path, "rb") as f:
        if config.format == "parquet":
            return pd.read_parquet(f, columns=columns, **options)
        if config.format == "csv":
            return pd.read_csv(f, usecols=columns, **options)
        if config.format == "zip":
            data = f.read()
            buffer = io.BytesIO(data)
            return _read_zip_archive(buffer, columns, options)
        raise ValueError(f"Unsupported S3 format: {config.format}")


def _read_zip_archive(path_or_buffer: Any, columns: Optional[list[str]], options: Dict[str, Any]) -> pd.DataFrame:
    """Extract and read a DataFrame from within a ZIP archive."""
    inner_format = options.pop("inner_format", "parquet")
    inner_pattern = options.pop("inner_path_pattern", None)
    inner_options = options.pop("inner_options", {})

    # Retain unused option keys for future extensibility without failing silently.
    if options:
        logger.debug("Unused ZIP read options provided: %s", options)

    with zipfile.ZipFile(path_or_buffer) as archive:
        member_name = _select_zip_member(archive.namelist(), inner_pattern, inner_format)
        if member_name is None:
            raise FileNotFoundError("No matching file found in ZIP archive.")
        with archive.open(member_name) as member:
            payload = member.read()
    buffer = io.BytesIO(payload)

    if inner_format == "parquet":
        return pd.read_parquet(buffer, columns=columns, **inner_options)
    if inner_format == "csv":
        buffer.seek(0)
        return pd.read_csv(buffer, usecols=columns, **inner_options)
    raise ValueError(f"Unsupported inner format for ZIP archive: {inner_format}")


def _select_zip_member(members: list[str], pattern: Optional[str], inner_format: str) -> Optional[str]:
    """Select a file entry from the ZIP archive matching the desired format."""
    if pattern:
        for name in members:
            if fnmatch(name, pattern):
                return name

    suffix_map = {"parquet": ".parquet", "csv": ".csv"}
    suffix = suffix_map.get(inner_format)
    if suffix:
        for name in members:
            if name.lower().endswith(suffix):
                return name

    for name in members:
        if not name.endswith("/"):
            return name
    return None

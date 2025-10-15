"""Configuration schemas for the AIS-RF fusion pipeline."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class S3Config(BaseModel):
    """Configuration for S3-backed data sources."""

    bucket: str
    key: str
    region: Optional[str] = None
    profile: Optional[str] = None


class DataSourceConfigSchema(BaseModel):
    """Generic data source configuration supporting local and S3 inputs."""

    type: str = Field(..., regex=r"^(s3|local)$")
    path: Optional[str]
    s3: Optional[S3Config]
    format: str = Field(..., regex=r"^(csv|parquet|zip)$")
    options: Dict[str, Any] = Field(default_factory=dict)

    @validator("path")
    def validate_path(cls, value: Optional[str], values):  # type: ignore[override]
        # TODO: Ensure path provided for local sources.
        return value


class OutputConfigSchema(BaseModel):
    """Configuration schema for output writer."""

    directory: str
    format: str = Field("parquet", regex=r"^(csv|parquet)$")
    partition_cols: List[str] = Field(default_factory=list)
    manifest_name: str = "manifest.json"


class AuditConfigSchema(BaseModel):
    """Configuration schema for audit logging."""

    directory: str
    filename: str = "pipeline_audit.log"


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration schema."""

    ais_source: DataSourceConfigSchema
    rf_source: DataSourceConfigSchema
    gap_fill: Dict[str, float]
    spoof_detection: Dict[str, float]
    indexing: Dict[str, str]
    output: OutputConfigSchema
    audit: AuditConfigSchema

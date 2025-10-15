"""Pydantic record schemas for AIS and RF inputs."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator


class AISRecord(BaseModel):
    """Schema for raw AIS messages."""

    mmsi: int = Field(..., description="Maritime Mobile Service Identity")
    ts_utc: datetime
    lat: float
    lon: float
    sog: Optional[float] = Field(None, description="Speed over ground in knots")
    cog: Optional[float] = Field(None, description="Course over ground in degrees")
    nav_status: Optional[str] = None
    position_accuracy: Optional[int] = None
    source_system: Optional[str] = None

    @validator("lat", "lon")
    def validate_coordinates(cls, value: float, field: Field) -> float:  # type: ignore[override]
        # TODO: Enforce coordinate bounds and raise validation error on failure.
        return value


class RFRecord(BaseModel):
    """Schema for RF geolocation detections."""

    rf_id: str
    ts_utc: datetime
    est_lat: float
    est_lon: float
    freq_band: Optional[str] = None
    rssi: Optional[float] = None
    toa: Optional[float] = None
    tdoa_cluster_id: Optional[str] = None
    geoloc_method: Optional[str] = None
    geo_uncertainty_m: Optional[float] = None

    @validator("est_lat", "est_lon")
    def validate_estimated_coordinates(cls, value: float, field: Field) -> float:  # type: ignore[override]
        # TODO: Enforce coordinate bounds and raise validation error on failure.
        return value

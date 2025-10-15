"""Output schemas and mapping utilities for N6 format."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class N6Record(BaseModel):
    """Schema representing an N6-compliant fused maritime record."""

    mmsi: int
    ts_utc: datetime
    lat: float
    lon: float
    source: str = Field(..., description="Source label: AIS, RF, or fused")
    fusion_method: Optional[str] = Field(None, description="Algorithm that produced this record")
    sog: Optional[float] = None
    cog: Optional[float] = None
    nav_status: Optional[str] = None
    position_accuracy: Optional[int] = None
    rf_reference_id: Optional[str] = None
    spoof_score: Optional[float] = None
    gap_fill_score: Optional[float] = None


def map_to_n6_schema(fused_df):
    """Convert fused DataFrame records into the N6 schema."""
    # TODO: Implement mapping using N6Record.parse_obj for validation.
    raise NotImplementedError("N6 mapping logic not yet implemented.")

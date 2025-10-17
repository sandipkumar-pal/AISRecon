"""AIS ingestion module tailored for zipped AIS deliveries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional

import pandas as pd

from utils.io import DataSourceConfig, read_dataframe
from utils.logging import get_logger


logger = get_logger(__name__)


DEFAULT_SOURCE_COLUMNS = [
    "LRIMOShipNo",
    "MMSI",
    "CallSign",
    "Draught",
    "MovementID",
    "MovementDateTime",
    "Latitude",
    "Longitude",
    "MoveStatus",
    "Speed",
    "Heading",
    "Destination",
    "PositionAccuracy",
]

DEFAULT_RENAME_MAP = {
    "LRIMOShipNo": "imo",
    "MMSI": "mmsi",
    "CallSign": "call_sign",
    "Draught": "draught",
    "MovementID": "movement_id",
    "MovementDateTime": "ts_utc",
    "Latitude": "lat",
    "Longitude": "lon",
    "MoveStatus": "nav_status",
    "Speed": "sog",
    "Heading": "cog",
    "Destination": "destination",
    "PositionAccuracy": "position_accuracy",
}


@dataclass
class AISIngestOptions:
    """Optional overrides to control AIS ingestion behaviour."""

    event_year: Optional[str] = None
    source_system: str = "AIS"


class AISIngestor:
    """Load AIS messages from configured storage backends and standardise schema."""

    def __init__(
        self,
        source_config: DataSourceConfig,
        options: Optional[AISIngestOptions] = None,
    ) -> None:
        self.source_config = source_config
        self.options = options or AISIngestOptions()

    def load(self, columns: Optional[list[str]] = None) -> pd.DataFrame:
        """Load AIS data as a DataFrame.

        Args:
            columns: Optional subset of *standardised* columns to return.

        Returns:
            Pandas DataFrame containing AIS observations with harmonised fields.
        """
        # TODO: Enforce schema validation using schemas.records.AISRecord.
        self._ensure_zip_defaults()
        raw_df = read_dataframe(self.source_config)
        logger.debug("Loaded raw AIS frame with shape: %s", raw_df.shape)
        standardised = self._standardise(raw_df)
        if columns:
            missing = sorted(set(columns) - set(standardised.columns))
            if missing:
                logger.warning("Requested columns %s missing from AIS payload", missing)
            standardised = standardised[[c for c in columns if c in standardised.columns]]
        # TODO: Apply lazy chunked loading if dataset exceeds memory constraints.
        return standardised

    def iter_batches(self, batch_size: int) -> Iterator[pd.DataFrame]:
        """Stream AIS data in batches for memory-efficient processing."""
        # TODO: Implement chunked iteration via pandas read_csv chunksize or parquet row groups.
        raise NotImplementedError("Batch iteration not yet implemented.")

    def _ensure_zip_defaults(self) -> None:
        """Inject default options when ingesting zipped AIS deliveries."""
        if self.source_config.format != "zip":
            return
        options = dict(self.source_config.options or {})
        options.setdefault("inner_format", "csv")
        if self.options.event_year and "inner_path_pattern" not in options:
            options["inner_path_pattern"] = f"*{self.options.event_year}*"
        inner_options = dict(options.get("inner_options") or {})
        inner_options.setdefault("sep", "\t")
        inner_options.setdefault("encoding", "latin-1")
        options["inner_options"] = inner_options
        self.source_config.options = options

    def _standardise(self, df: pd.DataFrame) -> pd.DataFrame:
        """Select, rename, and type-cast AIS raw columns."""
        available_columns = [c for c in DEFAULT_SOURCE_COLUMNS if c in df.columns]
        if not available_columns:
            logger.error("AIS payload missing expected columns: %s", DEFAULT_SOURCE_COLUMNS)
            return df
        subset = df[available_columns].copy()
        subset = subset.rename(columns={k: v for k, v in DEFAULT_RENAME_MAP.items() if k in subset.columns})
        subset = self._convert_types(subset)
        subset["source_system"] = self.options.source_system
        timestamps = pd.to_datetime(subset["ts_utc"], errors="coerce", utc=True)
        subset["event_date"] = timestamps.dt.date
        subset["event_year"] = (
            pd.Series(timestamps.dt.year, index=subset.index).astype("Int64")
        )
        epoch_ms = pd.Series(timestamps.view("int64"), index=subset.index) // 1_000_000
        subset["ts_utc"] = epoch_ms.where(~timestamps.isna(), pd.NA).astype("Int64")
        subset["timestamp"] = timestamps
        ordered_cols = [
            "mmsi",
            "imo",
            "call_sign",
            "ts_utc",
            "timestamp",
            "lat",
            "lon",
            "sog",
            "cog",
            "nav_status",
            "draught",
            "movement_id",
            "destination",
            "position_accuracy",
            "source_system",
            "event_date",
            "event_year",
        ]
        existing = [c for c in ordered_cols if c in subset.columns]
        remaining = [c for c in subset.columns if c not in existing]
        return subset[existing + remaining]

    def _convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast core AIS columns to canonical dtypes."""
        if "imo" in df.columns:
            df["imo"] = pd.to_numeric(df["imo"], errors="coerce").astype("Int64")
        if "mmsi" in df.columns:
            df["mmsi"] = df["mmsi"].astype(str).str.strip()
        numeric_fields = ["lat", "lon", "sog", "cog", "draught", "position_accuracy"]
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors="coerce")
        return df

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
        df = read_dataframe(self.source_config, columns=columns)
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
        options.setdefault("inner_format", "parquet")
        options.setdefault("inner_options", {})
        self.source_config.options = options

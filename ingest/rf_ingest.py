"""RF ingestion module for passive RF geolocation detections."""
from __future__ import annotations

from typing import Iterator, Optional

import pandas as pd

from utils.io import DataSourceConfig, read_dataframe


class RFIngestor:
    """Load RF detections from configured storage backends."""

    def __init__(self, source_config: DataSourceConfig) -> None:
        self.source_config = source_config

    def load(self, columns: Optional[list[str]] = None) -> pd.DataFrame:
        """Load RF data as a DataFrame."""
        # TODO: Validate schema using schemas.records.RFRecord.
        df = read_dataframe(self.source_config, columns=columns)
        # TODO: Apply RF-specific filtering for frequency bands or platforms.
        return df

    def iter_batches(self, batch_size: int) -> Iterator[pd.DataFrame]:
        """Stream RF detections in batches for incremental processing."""
        # TODO: Implement chunked iteration or push-based streaming from RF providers.
        raise NotImplementedError("Batch iteration not yet implemented.")

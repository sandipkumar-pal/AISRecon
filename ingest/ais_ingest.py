"""AIS ingestion module for loading AIS data from S3 or local sources."""
from __future__ import annotations

from typing import Iterator, Optional

import pandas as pd

from utils.io import DataSourceConfig, read_dataframe


class AISIngestor:
    """Load AIS messages from configured storage backends."""

    def __init__(self, source_config: DataSourceConfig) -> None:
        self.source_config = source_config

    def load(self, columns: Optional[list[str]] = None) -> pd.DataFrame:
        """Load AIS data as a DataFrame.

        Args:
            columns: Optional subset of columns to enforce schema alignment.

        Returns:
            Pandas DataFrame containing AIS observations.
        """
        # TODO: Enforce schema validation using schemas.records.AISRecord.
        self._ensure_zip_defaults()
        df = read_dataframe(self.source_config, columns=columns)
        # TODO: Apply lazy chunked loading if dataset exceeds memory constraints.
        return df

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

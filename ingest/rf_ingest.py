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
        self._ensure_zip_defaults()
        df = read_dataframe(self.source_config, columns=columns)
        # TODO: Apply RF-specific filtering for frequency bands or platforms.
        return df

    def iter_batches(self, batch_size: int) -> Iterator[pd.DataFrame]:
        """Stream RF detections in batches for incremental processing."""
        # TODO: Implement chunked iteration or push-based streaming from RF providers.
        raise NotImplementedError("Batch iteration not yet implemented.")

    def _ensure_zip_defaults(self) -> None:
        """Inject default options when ingesting zipped RF deliveries."""
        if self.source_config.format != "zip":
            return
        options = dict(self.source_config.options or {})
        target_member = "UNSEENLABS_SURMAR_20240706T155126Z_emitters.csv"
        if options.get("inner_path_pattern") is None:
            options["inner_path_pattern"] = target_member
        if options.get("inner_path_pattern") == target_member:
            options["inner_format"] = "csv"
        options.setdefault("inner_format", "csv")
        inner_options = dict(options.get("inner_options") or {})
        options["inner_options"] = inner_options
        self.source_config.options = options

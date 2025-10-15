"""Audit logging utilities for pipeline execution."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from utils.logging import get_logger


logger = get_logger(__name__)


@dataclass
class AuditConfig:
    """Configuration for audit trail persistence."""

    directory: str
    filename: str = "audit.log"


def record_audit_event(event: Dict[str, Any], config: AuditConfig) -> None:
    """Append audit event to the configured log destination."""
    target_dir = Path(config.directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    event_with_ts = {"timestamp": datetime.utcnow().isoformat(), **event}
    log_path = target_dir / config.filename
    logger.debug("Recording audit event to %s", log_path)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json_dumps(event_with_ts) + "\n")


def json_dumps(payload: Dict[str, Any]) -> str:
    """Serialize dictionary to JSON string."""
    # TODO: Replace with structured logging provider if needed.
    import json

    return json.dumps(payload)

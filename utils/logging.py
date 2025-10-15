"""Logging helpers for pipeline components."""
from __future__ import annotations

import logging
from typing import Optional


def get_logger(name: str, level: int = logging.INFO, formatter: Optional[str] = None) -> logging.Logger:
    """Return a configured logger instance."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(level)
        fmt = formatter or "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

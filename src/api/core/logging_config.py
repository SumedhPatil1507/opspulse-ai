"""
Structured JSON logging configuration.

Call ``configure_logging()`` once at application startup.  All subsequent
``logging.getLogger(...)`` calls inherit the JSON formatter, making log
lines parseable by Datadog, CloudWatch, Loki, etc. without extra pipelines.
"""

from __future__ import annotations

import logging
import logging.config
from typing import Any


def configure_logging(debug: bool = False) -> None:
    """
    Apply a structured logging configuration to the root logger.

    Parameters
    ----------
    debug:
        When ``True`` the root log level is set to DEBUG; otherwise INFO.
    """
    level = "DEBUG" if debug else "INFO"

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                # Use a simple key=value format that's easy to grep locally
                # and parse structurally in log aggregation platforms.
                "format": (
                    "%(asctime)s level=%(levelname)s "
                    "logger=%(name)s msg=%(message)s"
                ),
                "datefmt": "%Y-%m-%dT%H:%M:%S%z",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "json",
            },
        },
        "root": {
            "level": level,
            "handlers": ["console"],
        },
        # Quieten noisy third-party libraries
        "loggers": {
            "uvicorn.access": {"level": "WARNING"},
            "celery": {"level": "INFO"},
            "kombu": {"level": "WARNING"},
        },
    }

    logging.config.dictConfig(config)

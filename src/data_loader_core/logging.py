"""Structured logging configuration and utilities.

This module configures structlog for the framework, providing structured logging
with context variables, JSON output, and consistent formatting across all components.
"""

import logging.config
import os
import sys
from collections.abc import Mapping
from enum import Enum
from typing import Any

import structlog
from structlog.contextvars import merge_contextvars
from structlog.dev import ConsoleRenderer, set_exc_info
from structlog.typing import EventDict, WrappedLogger


def _no_op_structlog_processor(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    return event_dict


class ConsoleMode(str, Enum):
    """Enum of possible ConsoleMode options."""

    OFF = ("off",)
    AUTO = ("auto",)
    FORCE = "force"


class LoggingHandler(str, Enum):
    """Enum of possible LoggingHandler options."""

    TEXT = "console-text"
    JSON = "console-json"


class LoggingLevel(str, Enum):
    """Enum of possible LoggingLevel options."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def _default_package_log_level(
    package_log_levels: dict[str, LoggingLevel],
    logger_name: str,
    logging_level: LoggingLevel,
) -> None:
    if logger_name not in package_log_levels:
        package_log_levels[logger_name] = logging_level


def parse_logging_config() -> Mapping[str, Any]:
    """Parse configuration values required for logging."""
    logging_level = LoggingLevel(os.getenv("OC_LOGGING_LEVEL", "INFO").upper())
    package_log_levels = _parse_package_log_levels(
        os.getenv("OC_LOGGING_PACKAGE_LEVELS", None),
    )
    _default_package_log_level(
        package_log_levels,
        "gunicorn.error",
        LoggingLevel.WARNING,
    )
    _default_package_log_level(package_log_levels, "botocore", LoggingLevel.WARNING)
    _default_package_log_level(package_log_levels, "boto3", LoggingLevel.WARNING)
    _default_package_log_level(package_log_levels, "urllib3", LoggingLevel.WARNING)
    _default_package_log_level(package_log_levels, "httpx", LoggingLevel.INFO)

    logging_handler = LoggingHandler(os.getenv("OC_LOGGING_HANDLER", "console-text"))
    console_mode = (
        ConsoleMode(os.environ.get("OC_LOGGING_CONSOLE_COLOR", ""))
        if "OC_LOGGING_CONSOLE_COLOR" in os.environ
        else ConsoleMode.AUTO
    )
    return {
        "logging_level": logging_level,
        "package_log_levels": package_log_levels,
        "logging_handler": logging_handler,
        "console_mode": console_mode,
    }


def configure_logging(
    logging_level: LoggingLevel,
    package_log_levels: Mapping[str, LoggingLevel],
    logging_handler: LoggingHandler = LoggingHandler.TEXT,
    console_mode: ConsoleMode = ConsoleMode.AUTO,
) -> None:
    """Configure logging using structlog.

    Patches logging to forward all stdlib logging onto structlog.
    """
    # Timestamp format for structlog log messages.
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    # stdlib chain of handlers to decorate log messages with additional information.
    pre_chain = [
        # Add the log level and a timestamp to the event_dict if the log
        # entry is not from structlog.
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
    ]

    if console_mode == ConsoleMode.AUTO:
        console_formatter = (
            "console-color"
            if (
                sys.stdout is not None
                and hasattr(sys.stdout, "isatty")
                and sys.stdout.isatty()
            )
            else "console-no-color"
        )
    elif console_mode == ConsoleMode.FORCE:  # pragma: no cover
        console_formatter = "console-color"  # pragma: no cover
    else:  # pragma: no cover
        console_formatter = "console-no-color"  # pragma: no cover

    # stdlib logging configuration to forward log messages to structlog.
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "console-no-color": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        ConsoleRenderer(colors=False),
                    ],
                    "foreign_pre_chain": pre_chain,
                },
                "console-color": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        ConsoleRenderer(colors=True),
                    ],
                    "foreign_pre_chain": pre_chain,
                },
                "json": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        structlog.processors.JSONRenderer(),
                    ],
                    "foreign_pre_chain": pre_chain,
                },
            },
            "handlers": {
                "console-text": {
                    "level": "DEBUG",
                    "class": "logging.StreamHandler",
                    "formatter": console_formatter,
                },
                "console-json": {
                    "level": "DEBUG",
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                },
            },
            "loggers": {
                **{
                    pkg: {
                        "handlers": [logging_handler.value],
                        "level": level.value,
                        "propagate": False,
                    }
                    for pkg, level in package_log_levels.items()
                },
                "": {
                    "handlers": [logging_handler.value],
                    "level": logging_level.value,
                    "propagate": True,
                },
            },
            "root": {
                "handlers": [logging_handler.value],
                "level": logging_level.value,
            },
        },
    )

    # Set up structlog
    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.processors.CallsiteParameterAdder(
                parameters={
                    structlog.processors.CallsiteParameter.PROCESS,
                    structlog.processors.CallsiteParameter.PROCESS_NAME,
                },
            ),
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            set_exc_info,
            (
                structlog.processors.dict_tracebacks
                if logging_handler != LoggingHandler.TEXT
                else _no_op_structlog_processor
            ),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),  # Required for stdlib/gunicorn
        wrapper_class=structlog.stdlib.BoundLogger,  # Required for stdlib/gunicorn
        cache_logger_on_first_use=True,  # Required for stdlib/gunicorn
    )


def _parse_package_log_levels(
    package_log_levels: dict[str, LoggingLevel] | dict[str, str] | str | None,
) -> dict[str, LoggingLevel]:
    if isinstance(package_log_levels, dict):
        return {
            k: v if isinstance(v, LoggingLevel) else LoggingLevel(v.upper())
            for k, v in package_log_levels.items()
        }
    if isinstance(package_log_levels, str) and ":" in package_log_levels:
        return {
            k: LoggingLevel(v.upper())
            for k, v in (s.split(":") for s in package_log_levels.split(","))
        }
    if package_log_levels in ("", None):
        return {}

    raise ValueError(  # noqa: TRY003
        f"Cannot parse package log levels: '{package_log_levels}'",
    )


def setup_logging() -> None:
    """Set up logging with default configuration."""
    config = parse_logging_config()
    configure_logging(**config)

"""Custom exceptions for the loader framework."""


class LoaderError(Exception):
    """Base exception for loader-related errors."""

    pass


class ConfigurationError(LoaderError):
    """Raised when there's a configuration-related error."""

    pass


class BatchError(LoaderError):
    """Raised when there's an error processing a batch."""

    pass


class SQSProcessingError(LoaderError):
    """Raised when there's an error processing SQS messages."""

    pass
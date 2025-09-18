"""Unit tests for the exceptions module."""

import pytest

from data_loader_core.exceptions import (
    BatchError,
    ConfigurationError,
    LoaderError,
    SQSProcessingError,
)


class TestLoaderExceptions:
    """Test cases for loader exceptions."""

    def test_loader_error_inheritance(self):
        """Test that LoaderError is the base exception."""
        error = LoaderError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_configuration_error_inheritance(self):
        """Test that ConfigurationError inherits from LoaderError."""
        error = ConfigurationError("config error")
        assert isinstance(error, LoaderError)
        assert str(error) == "config error"

    def test_batch_error_inheritance(self):
        """Test that BatchError inherits from LoaderError."""
        error = BatchError("batch error")
        assert isinstance(error, LoaderError)
        assert str(error) == "batch error"

    def test_sqs_processing_error_inheritance(self):
        """Test that SQSProcessingError inherits from LoaderError."""
        error = SQSProcessingError("sqs error")
        assert isinstance(error, LoaderError)
        assert str(error) == "sqs error"

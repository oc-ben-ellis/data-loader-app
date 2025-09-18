"""Tests for application entry point.

This module contains unit tests for the main application functionality.
"""

from data_loader_app.main import main_async


class TestMainApplication:
    """Test main application functionality."""

    def test_main_async_import(self) -> None:
        """Test that main_async function can be imported."""
        assert callable(main_async)

    def test_cli_import(self) -> None:
        """Test that CLI functions can be imported."""
        from data_loader_app.main import (
            health_command,
            main,
            run_command,
            show_help,
        )

        assert callable(main)
        assert callable(run_command)
        assert callable(health_command)
        assert callable(show_help)

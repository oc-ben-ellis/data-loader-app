"""Unit tests for CLI functionality.

This module contains unit tests for the command-line interface,
including argument parsing and command dispatch.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from data_loader_app.main import (
    generate_run_id,
    health_command,
    main,
    run_command,
    show_help,
)


class TestCLI:
    """Test CLI functionality."""

    def test_generate_run_id(self) -> None:
        """Test run ID generation."""
        config_id = "test_config"
        run_id = generate_run_id(config_id)
        
        assert run_id.startswith("loader_test_config_")
        assert len(run_id) > len("loader_test_config_")

    def test_show_help(self, capsys) -> None:
        """Test help display."""
        show_help()
        captured = capsys.readouterr()
        
        assert "OpenCorporates Data loader" in captured.out
        assert "Commands:" in captured.out
        assert "run" in captured.out
        assert "health" in captured.out

    def test_main_with_help(self, capsys) -> None:
        """Test main function with help command."""
        with patch.object(sys, "argv", ["main.py", "--help"]):
            main()
        
        captured = capsys.readouterr()
        assert "OpenCorporates Data loader" in captured.out

    def test_main_with_version(self, capsys) -> None:
        """Test main function with version command."""
        with patch.object(sys, "argv", ["main.py", "--version"]):
            main()
        
        captured = capsys.readouterr()
        assert "data-loader-app, version 0.1.0" in captured.out

    def test_main_with_invalid_command(self, capsys) -> None:
        """Test main function with invalid command."""
        with patch.object(sys, "argv", ["main.py", "invalid"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_main_with_insufficient_args(self, capsys) -> None:
        """Test main function with insufficient arguments."""
        with patch.object(sys, "argv", ["main.py"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("data_loader_app.main.create_run_config")
    @patch("data_loader_app.main.configure_logging")
    @patch("data_loader_app.main.main_async")
    def test_run_command_basic(self, mock_main_async, mock_configure_logging, mock_create_run_config) -> None:
        """Test run command with basic arguments."""
        # Mock the config
        mock_config = MagicMock()
        mock_config.data_registry_id = None
        mock_config.stage = None
        mock_config.step = None
        mock_config.log_level = "INFO"
        mock_config.dev_mode = False
        mock_create_run_config.return_value = mock_config
        
        # Mock environment variable
        with patch.dict("os.environ", {"OC_DATA_PIPELINE_DATA_REGISTRY_ID": "test_registry"}):
            with pytest.raises(SystemExit) as exc_info:
                run_command(["--data-registry-id", "test_registry"])
            # Should exit with error due to missing stage/step
            assert exc_info.value.code == 1

    @patch("data_loader_app.main.create_health_config")
    @patch("data_loader_app.main.configure_logging")
    @patch("data_loader_app.main.create_health_app")
    @patch("data_loader_app.main.make_server")
    def test_health_command_basic(self, mock_make_server, mock_create_health_app, mock_configure_logging, mock_create_health_config) -> None:
        """Test health command with basic arguments."""
        # Mock the config
        mock_config = MagicMock()
        mock_config.host = "127.0.0.1"
        mock_config.port = 8080
        mock_config.log_level = "INFO"
        mock_config.dev_mode = False
        mock_create_health_config.return_value = mock_config
        
        # Mock the health app
        mock_app = MagicMock()
        mock_create_health_app.return_value = mock_app
        
        # Mock the server
        mock_server = MagicMock()
        mock_make_server.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_make_server.return_value.__exit__ = MagicMock()
        
        # Mock KeyboardInterrupt to exit the server loop
        mock_server.serve_forever.side_effect = KeyboardInterrupt()
        
        health_command(["--port", "8080"])

    def test_run_command_missing_data_registry_id(self, capsys) -> None:
        """Test run command with missing data registry ID."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(SystemExit) as exc_info:
                run_command([])
            assert exc_info.value.code == 1

    def test_run_command_missing_stage_when_data_registry_id_provided(self, capsys) -> None:
        """Test run command with data registry ID but missing stage."""
        with patch("data_loader_app.main.create_run_config") as mock_create_run_config:
            mock_config = MagicMock()
            mock_config.data_registry_id = "test_registry"
            mock_config.stage = None
            mock_config.step = None
            mock_create_run_config.return_value = mock_config
            
            with pytest.raises(SystemExit) as exc_info:
                run_command(["--data-registry-id", "test_registry"])
            assert exc_info.value.code == 1

    def test_run_command_missing_step_when_data_registry_id_provided(self, capsys) -> None:
        """Test run command with data registry ID and stage but missing step."""
        with patch("data_loader_app.main.create_run_config") as mock_create_run_config:
            mock_config = MagicMock()
            mock_config.data_registry_id = "test_registry"
            mock_config.stage = "test_stage"
            mock_config.step = None
            mock_create_run_config.return_value = mock_config
            
            with pytest.raises(SystemExit) as exc_info:
                run_command(["--data-registry-id", "test_registry", "--stage", "test_stage"])
            assert exc_info.value.code == 1

"""Integration tests for configuration loading.

This module contains integration tests for the configuration system,
including YAML parsing, strategy registry, and basic functionality.
"""

import tempfile
from pathlib import Path

import pytest
import yaml
from oc_pipeline_bus.config import DataPipelineConfig
from oc_pipeline_bus.strategy_registry import StrategyFactoryRegistry

from data_loader_core.core import DataRegistryloaderConfig
from data_loader_core.strategy_registration import create_strategy_registry


class TestConfigIntegration:
    """Integration tests for configuration loading."""

    @pytest.fixture
    def sample_config_yaml(self) -> str:
        """Sample YAML configuration for testing."""
        return """
config_id: test_source

concurrency: 5
target_queue_size: 50

loader:
  api_loader:
    meta_load_name: test_api_loader
    api_config: test_api

locators:
  - api_locator:
      api_config: test_api
      base_url: "https://api.example.com/data"
      date_start: "2023-01-01"
      date_end: "2023-12-31"

processors:
  - data_processor:
      output_format: json
      validation_rules:
        - required_fields: ["id", "name", "timestamp"]

storage:
  type: s3
  bucket: test-bucket
  prefix: test-data/

api_configs:
  test_api:
    credentials_secret: sample-api-credentials
    timeout: 30
    retry_attempts: 3
"""

    @pytest.fixture
    def temp_config_dir(self, sample_config_yaml: str) -> Path:
        """Create a temporary directory with sample configuration files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create the main configuration file
            config_file = config_dir / "orchestration.yaml"
            config_file.write_text(sample_config_yaml)
            
            yield config_dir

    def test_strategy_registry_creation(self) -> None:
        """Test that strategy registry can be created."""
        registry = create_strategy_registry()
        assert isinstance(registry, StrategyFactoryRegistry)

    def test_data_pipeline_config_initialization(self, temp_config_dir: Path) -> None:
        """Test DataPipelineConfig initialization."""
        registry = create_strategy_registry()
        config = DataPipelineConfig(
            strategy_registry=registry,
            local_config_dir=str(temp_config_dir),
        )
        
        assert config.strategy_registry == registry
        assert config.local_config_dir == str(temp_config_dir)

    def test_config_loading(self, temp_config_dir: Path) -> None:
        """Test loading configuration from YAML files."""
        registry = create_strategy_registry()
        config = DataPipelineConfig(
            strategy_registry=registry,
            local_config_dir=str(temp_config_dir),
        )
        
        # Load the configuration
        loaded_config = config.load_config(
            DataRegistryloaderConfig,
            data_registry_id="test_source",
            step="test_step",
        )
        
        assert loaded_config is not None
        assert loaded_config.config_id == "test_source"
        assert loaded_config.concurrency == 5
        assert loaded_config.target_queue_size == 50

    def test_config_loading_with_invalid_id(self, temp_config_dir: Path) -> None:
        """Test loading configuration with invalid data registry ID."""
        registry = create_strategy_registry()
        config = DataPipelineConfig(
            strategy_registry=registry,
            local_config_dir=str(temp_config_dir),
        )
        
        # Try to load with invalid ID
        with pytest.raises(KeyError):
            config.load_config(
                DataRegistryloaderConfig,
                data_registry_id="invalid_id",
                step="test_step",
            )

    def test_config_loading_with_missing_step(self, temp_config_dir: Path) -> None:
        """Test loading configuration with missing step."""
        registry = create_strategy_registry()
        config = DataPipelineConfig(
            strategy_registry=registry,
            local_config_dir=str(temp_config_dir),
        )
        
        # Try to load without step
        with pytest.raises(KeyError):
            config.load_config(
                DataRegistryloaderConfig,
                data_registry_id="test_source",
                step=None,
            )

    def test_yaml_parsing_validation(self, sample_config_yaml: str) -> None:
        """Test that YAML configuration can be parsed and validated."""
        config_data = yaml.safe_load(sample_config_yaml)
        
        # Validate required fields
        assert "config_id" in config_data
        assert "concurrency" in config_data
        assert "target_queue_size" in config_data
        assert "loader" in config_data
        assert "locators" in config_data
        assert "processors" in config_data
        assert "storage" in config_data
        assert "api_configs" in config_data
        
        # Validate data types
        assert isinstance(config_data["config_id"], str)
        assert isinstance(config_data["concurrency"], int)
        assert isinstance(config_data["target_queue_size"], int)
        assert isinstance(config_data["loader"], dict)
        assert isinstance(config_data["locators"], list)
        assert isinstance(config_data["processors"], list)
        assert isinstance(config_data["storage"], dict)
        assert isinstance(config_data["api_configs"], dict)

    def test_config_with_minimal_fields(self) -> None:
        """Test configuration with minimal required fields."""
        minimal_config = """
config_id: minimal_test
concurrency: 1
target_queue_size: 10
loader:
  basic_loader:
    meta_load_name: basic
locators:
  - basic_locator:
      base_url: "https://example.com"
processors:
  - basic_processor:
      output_format: json
storage:
  type: file
  path: /tmp/test
"""
        
        config_data = yaml.safe_load(minimal_config)
        assert config_data["config_id"] == "minimal_test"
        assert config_data["concurrency"] == 1
        assert config_data["target_queue_size"] == 10

"""Unit tests for the core module."""

import pytest

from data_loader_core.core import DataRegistryLoaderConfig


class TestDataRegistryLoaderConfig:
    """Test cases for DataRegistryLoaderConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DataRegistryLoaderConfig()
        
        assert config.config_id == ""
        assert config.batch_max_size == 1_000_000

    def test_custom_config(self):
        """Test custom configuration values."""
        config = DataRegistryLoaderConfig(
            config_id="us_fl",
            batch_max_size=500_000,
        )
        
        assert config.config_id == "us_fl"
        assert config.batch_max_size == 500_000
"""Integration tests for the loader module."""

import json
import os
import tempfile
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from data_loader_core.core import DataRegistryLoaderConfig
from data_loader_core.loader import run_loader
from data_loader_app.app_config import loaderConfig


class TestLoaderIntegration:
    """Integration tests for the loader functionality."""

    @pytest.mark.asyncio
    async def test_loader_integration_with_mocked_services(self):
        """Test loader integration with mocked external services."""
        # Create temporary config directory
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = os.path.join(temp_dir, "config")
            os.makedirs(config_dir)
            
            # Create a simple loader config
            config_file = os.path.join(config_dir, "loader_config.yaml")
            with open(config_file, "w") as f:
                f.write("""
data_source_id: us_fl

loader:
  batch_max_size: 100
""")
            
            # Mock app config
            app_config = MagicMock(spec=loaderConfig)
            
            # Mock environment variables
            env_vars = {
                "OC_DATA_PIPELINE_LOADER_SQS_URL": "http://test-queue",
                "OC_DATA_PIPELINE_DATA_REGISTRY_ID": "us_fl",
                "OC_DATA_PIPELINE_STAGE": "transformed",
                "OC_DATA_PIPELINE_STORAGE_S3_URL": "s3://test-bucket",
                "OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL": "http://test-orchestration-queue",
            }
            
            with patch.dict(os.environ, env_vars):
                with patch("data_loader_core.loader.DataPipelineBus") as mock_bus_class:
                    mock_bus = MagicMock()
                    mock_bus_class.return_value = mock_bus
                    
                    # Mock the change event
                    mock_change_event = MagicMock()
                    mock_change_event.event = "record_added"
                    mock_change_event.stage = "transformed"
                    mock_change_event.data_registry_id = "us_fl"
                    mock_bus.get_change_event.return_value = mock_change_event
                    
                    # Mock SQS client
                    with patch("boto3.client") as mock_boto:
                        mock_sqs = MagicMock()
                        mock_boto.return_value = mock_sqs
                        
                        # Mock queue attributes - no messages
                        mock_sqs.get_queue_attributes.return_value = {
                            "Attributes": {"ApproximateNumberOfMessages": "0"}
                        }
                        
                        # Mock the pipeline config loading
                        with patch("oc_pipeline_bus.config.DataPipelineConfig") as mock_config_class:
                            mock_config = MagicMock()
                            mock_config_class.return_value = mock_config
                            
                            # Mock config loading
                            mock_loader_config = DataRegistryLoaderConfig(
                                config_id="us_fl",
                                batch_max_size=100
                            )
                            mock_config.load_config.return_value = mock_loader_config
                            
                            # Run the loader
                            await run_loader(
                                app_config=app_config,
                                loader_config=mock_loader_config,
                                data_registry_id="us_fl",
                                stage="transformed",
                            )
                            
                            # Verify that the pipeline bus was created with correct parameters
                            mock_bus_class.assert_called_with(
                                stage="loaded", 
                                data_registry_id="us_fl"
                            )
                            
                            # Verify that get_change_event was called
                            mock_bus.get_change_event.assert_called_once()
                            
                            # Verify that queue attributes were checked
                            mock_sqs.get_queue_attributes.assert_called()

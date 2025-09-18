"""Unit tests for the loader module."""

import json
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from data_loader_core.exceptions import ConfigurationError, LoaderError
from data_loader_core.loader import create_and_upload_batch, process_sqs_messages, run_loader
from data_loader_core.core import DataRegistryLoaderConfig
from data_loader_app.app_config import loaderConfig


class TestRunLoader:
    """Test cases for the run_loader function."""

    @pytest.mark.asyncio
    async def test_run_loader_missing_sqs_url(self):
        """Test that run_loader raises ConfigurationError when SQS URL is missing."""
        app_config = MagicMock(spec=loaderConfig)
        loader_config = DataRegistryLoaderConfig()
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigurationError, match="OC_DATA_PIPELINE_LOADER_SQS_URL"):
                await run_loader(
                    app_config=app_config,
                    loader_config=loader_config,
                    data_registry_id="us_fl",
                    stage="transformed",
                )

    @pytest.mark.asyncio
    async def test_run_loader_success(self):
        """Test successful loader execution."""
        app_config = MagicMock(spec=loaderConfig)
        loader_config = DataRegistryLoaderConfig(batch_max_size=100)
        
        with patch.dict(os.environ, {"OC_DATA_PIPELINE_LOADER_SQS_URL": "http://test-queue"}):
            with patch("data_loader_core.loader.DataPipelineBus") as mock_bus_class:
                mock_bus = MagicMock()
                mock_bus_class.return_value = mock_bus
                
                # Mock the change event
                mock_change_event = MagicMock()
                mock_change_event.event = "record_added"
                mock_change_event.stage = "transformed"
                mock_change_event.data_registry_id = "us_fl"
                mock_bus.get_change_event.return_value = mock_change_event
                
                # Mock process_sqs_messages
                with patch("data_loader_core.loader.process_sqs_messages") as mock_process:
                    await run_loader(
                        app_config=app_config,
                        loader_config=loader_config,
                        data_registry_id="us_fl",
                        stage="transformed",
                    )
                    
                    mock_process.assert_called_once()


class TestProcessSqsMessages:
    """Test cases for the process_sqs_messages function."""

    @pytest.mark.asyncio
    async def test_process_sqs_messages_no_messages(self):
        """Test processing when there are no messages in the queue."""
        mock_bus = MagicMock()
        sqs_queue_url = "http://test-queue"
        
        with patch("boto3.client") as mock_boto:
            mock_sqs = MagicMock()
            mock_boto.return_value = mock_sqs
            
            # Mock queue attributes - no messages
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"ApproximateNumberOfMessages": "0"}
            }
            
            await process_sqs_messages(
                data_pipeline_bus=mock_bus,
                sqs_queue_url=sqs_queue_url,
                batch_max_size=100,
                data_registry_id="us_fl",
            )
            
            # Should not call receive_message
            mock_sqs.receive_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_sqs_messages_with_messages(self):
        """Test processing when there are messages in the queue."""
        mock_bus = MagicMock()
        sqs_queue_url = "http://test-queue"
        
        # Mock record data
        mock_record_data = {"company_number": "12345", "name": "Test Company"}
        mock_bus.get_snapshot_json.return_value = mock_record_data
        
        with patch("boto3.client") as mock_boto:
            mock_sqs = MagicMock()
            mock_boto.return_value = mock_sqs
            
            # Mock queue attributes - 2 messages
            mock_sqs.get_queue_attributes.return_value = {
                "Attributes": {"ApproximateNumberOfMessages": "2"}
            }
            
            # Mock receive_message response
            mock_sqs.receive_message.return_value = {
                "Messages": [
                    {
                        "Body": json.dumps({
                            "event": "record_added",
                            "record_id": {"ocid": "ocid:v1:co:test1", "bid": "bid:v1:us_fl:test1"}
                        }),
                        "ReceiptHandle": "handle1"
                    },
                    {
                        "Body": json.dumps({
                            "event": "record_added", 
                            "record_id": {"ocid": "ocid:v1:co:test2", "bid": "bid:v1:us_fl:test2"}
                        }),
                        "ReceiptHandle": "handle2"
                    }
                ]
            }
            
            with patch("data_loader_core.loader.create_and_upload_batch") as mock_create_batch:
                await process_sqs_messages(
                    data_pipeline_bus=mock_bus,
                    sqs_queue_url=sqs_queue_url,
                    batch_max_size=100,
                    data_registry_id="us_fl",
                )
                
                # Should call create_and_upload_batch once with 2 records
                mock_create_batch.assert_called_once()
                call_args = mock_create_batch.call_args
                assert len(call_args[1]["batch_records"]) == 2


class TestCreateAndUploadBatch:
    """Test cases for the create_and_upload_batch function."""

    @pytest.mark.asyncio
    async def test_create_and_upload_batch_success(self):
        """Test successful batch creation and upload."""
        mock_bus = MagicMock()
        mock_bus._generate_bid.return_value = "bid:v1:us_fl:test123"
        
        batch_records = [
            {
                "record_id": {"ocid": "ocid:v1:co:test1", "bid": "bid:v1:us_fl:test1"},
                "data": {"company_number": "12345", "name": "Test Company 1"},
                "message_receipt_handle": "handle1"
            },
            {
                "record_id": {"ocid": "ocid:v1:co:test2", "bid": "bid:v1:us_fl:test2"},
                "data": {"company_number": "67890", "name": "Test Company 2"},
                "message_receipt_handle": "handle2"
            }
        ]
        
        await create_and_upload_batch(
            data_pipeline_bus=mock_bus,
            batch_records=batch_records,
            batch_number=1,
            data_registry_id="us_fl",
        )
        
        # Verify add_bundle_resource was called
        mock_bus.add_bundle_resource.assert_called_once()
        call_args = mock_bus.add_bundle_resource.call_args
        
        # Check the resource name matches expected pattern
        resource_name = call_args[1]["resource_name"]
        assert resource_name.startswith("batch_")
        assert resource_name.endswith(".jsonl")
        
        # Check the metadata
        metadata = call_args[1]["metadata"]
        assert metadata["batch_number"] == 1
        assert metadata["record_count"] == 2
        assert metadata["data_registry_id"] == "us_fl"
        
        # Verify complete_bundle was called
        mock_bus.complete_bundle.assert_called_once()
        
        # Verify mark_snapshot_as_loaded was called for each record
        assert mock_bus.mark_snapshot_as_loaded.call_count == 2

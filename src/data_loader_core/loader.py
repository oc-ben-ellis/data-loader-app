"""Loader execution engine for processing transformed records.

This module contains the core loader logic that orchestrates the data processing
from transformed stage to loaded stage using the pipeline bus and SQS message processing.
"""

import asyncio
import json
import os
import secrets
from datetime import UTC, datetime
from typing import Any

import boto3
import structlog

from data_loader_app.app_config import loaderConfig
from data_loader_core.core import DataRegistryLoaderConfig
from data_loader_core.exceptions import LoaderError, ConfigurationError
from oc_pipeline_bus import DataPipelineBus
from oc_pipeline_bus.identifiers import SnapshotId

# Get logger for this module
logger = structlog.get_logger(__name__)


async def run_loader(
    app_config: loaderConfig,
    loader_config: DataRegistryLoaderConfig
) -> None:
    """Run the async loader to process transformed records to loaded stage.

    Args:
        app_config: Application configuration with storage and credentials.
        loader_config: Loader configuration with batch settings.
        data_registry_id: The data registry ID being processed.
        stage: The current stage (should be "transformed" for loader).
    """

    logger.info(
        "LOADER_STARTING",
        batch_max_size=loader_config.batch_max_size,
    )

    # Create pipeline bus for loaded stage
    data_pipeline_bus = DataPipelineBus()
    
    data_registry_id = data_pipeline_bus.get_data_registry_id()
    stage = data_pipeline_bus.get_stage()

    # Get SQS queue URL from environment
    sqs_queue_url = os.environ.get("OC_DATA_PIPELINE_LOADER_SQS_URL")
    if not sqs_queue_url:
        raise ConfigurationError("OC_DATA_PIPELINE_LOADER_SQS_URL environment variable is required")

    try:
        # Get change event to understand what triggered this run
        change_event = data_pipeline_bus.get_change_event()
        logger.info(
            "LOADER_TRIGGERED",
            event=change_event.event,
            stage=change_event.stage,
            data_registry_id=change_event.data_registry_id,
        )

        # Process messages from SQS queue
        await process_sqs_messages(
            data_pipeline_bus=data_pipeline_bus,
            sqs_queue_url=sqs_queue_url,
            batch_max_size=loader_config.batch_max_size,
            data_registry_id=data_registry_id,
        )

        logger.info("LOADER_COMPLETED", data_registry_id=data_registry_id, stage=stage)

    except Exception as e:
        logger.exception(
            "LOADER_ERROR", data_registry_id=data_registry_id, error=str(e)
        )
        raise  # Fail fast as requested


async def process_sqs_messages(
    data_pipeline_bus: DataPipelineBus,
    sqs_queue_url: str,
    batch_max_size: int,
    data_registry_id: str,
) -> None:
    """Process messages from the loader SQS queue and create JSONL batches.

    Args:
        data_pipeline_bus: Pipeline bus instance for loaded stage.
        sqs_queue_url: URL of the SQS queue containing record_added messages.
        batch_max_size: Maximum number of records to include in a batch.
        data_registry_id: The data registry ID being processed.
    """
    sqs_client = boto3.client("sqs")
    
    # Get queue attributes to check message count
    queue_attrs = sqs_client.get_queue_attributes(
        QueueUrl=sqs_queue_url,
        AttributeNames=["ApproximateNumberOfMessages"]
    )
    message_count = int(queue_attrs["Attributes"]["ApproximateNumberOfMessages"])
    
    logger.info(
        "SQS_QUEUE_STATUS",
        queue_url=sqs_queue_url,
        message_count=message_count,
        batch_max_size=batch_max_size,
    )

    if message_count == 0:
        logger.info("NO_MESSAGES_TO_PROCESS")
        return

    # Process messages in batches
    records_processed = 0
    batch_number = 1
    
    while message_count > 0:
        # Receive messages (up to 10 at a time from SQS)
        messages_to_receive = min(10, message_count, batch_max_size - records_processed)
        
        response = sqs_client.receive_message(
            QueueUrl=sqs_queue_url,
            MaxNumberOfMessages=messages_to_receive,
            WaitTimeSeconds=1,  # Short poll
        )
        
        messages = response.get("Messages", [])
        if not messages:
            break
            
        logger.info(
            "RECEIVED_MESSAGES",
            count=len(messages),
            batch_number=batch_number,
        )

        # Process this batch of messages
        batch_records = []
        for message in messages:
            try:
                # Parse the message body
                message_body = json.loads(message["Body"])
                
                # Extract record information
                if "record_id" in message_body:
                    record_id = message_body["record_id"]
                    ocid = record_id["ocid"]
                    bid = record_id["bid"]
                    
                    # Get the transformed record
                    record_data = data_pipeline_bus.get_snapshot_json(ocid, bid, stage="transformed")
                    
                    batch_records.append({
                        "record_id": record_id,
                        "data": record_data,
                        "message_receipt_handle": message["ReceiptHandle"],
                    })
                    
                else:
                    logger.warning("INVALID_MESSAGE_FORMAT", message_body=message_body)
                    
            except Exception as e:
                logger.exception("MESSAGE_PROCESSING_ERROR", message=message, error=str(e))
                # Continue processing other messages

        if batch_records:
            # Create and upload JSONL batch
            await create_and_upload_batch(
                data_pipeline_bus=data_pipeline_bus,
                batch_records=batch_records,
                batch_number=batch_number,
                data_registry_id=data_registry_id,
            )
            
            # Delete processed messages from SQS
            for record in batch_records:
                sqs_client.delete_message(
                    QueueUrl=sqs_queue_url,
                    ReceiptHandle=record["message_receipt_handle"]
                )
            
            records_processed += len(batch_records)
            batch_number += 1
            
            logger.info(
                "BATCH_PROCESSED",
                batch_number=batch_number - 1,
                records_in_batch=len(batch_records),
                total_records_processed=records_processed,
            )

        # Check if we've reached the batch size limit
        if records_processed >= batch_max_size:
            logger.info("BATCH_SIZE_LIMIT_REACHED", records_processed=records_processed)
            break
            
        # Update message count for next iteration
        queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=sqs_queue_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )
        message_count = int(queue_attrs["Attributes"]["ApproximateNumberOfMessages"])


async def create_and_upload_batch(
    data_pipeline_bus: DataPipelineBus,
    batch_records: list[dict[str, Any]],
    batch_number: int,
    data_registry_id: str,
) -> None:
    """Create a JSONL batch file and upload it to the loaded stage.

    Args:
        data_pipeline_bus: Pipeline bus instance for loaded stage.
        batch_records: List of records to include in the batch.
        batch_number: Sequential batch number for naming.
        data_registry_id: The data registry ID being processed.
    """
    # Generate batch filename with timestamp and suffix
    timestamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    suffix = secrets.token_hex(4)  # 8 character random suffix
    batch_filename = f"batch_{timestamp}-{suffix}.jsonl"
    
    logger.info(
        "CREATING_BATCH",
        batch_filename=batch_filename,
        record_count=len(batch_records),
        batch_number=batch_number,
    )

    # Create JSONL content
    jsonl_lines = []
    for record in batch_records:
        jsonl_lines.append(json.dumps(record["data"], ensure_ascii=False))
    
    jsonl_content = "\n".join(jsonl_lines) + "\n"
    
    # Generate a BID for this batch
    batch_bid = data_pipeline_bus._generate_bid()
    
    # Upload the batch file
    import io
    data_pipeline_bus.add_bundle_resource(
        bid=batch_bid,
        resource_name=batch_filename,
        metadata={
            "batch_number": batch_number,
            "record_count": len(batch_records),
            "created_at": datetime.now(UTC).isoformat(),
            "data_registry_id": data_registry_id,
        },
        read_stream=io.BytesIO(jsonl_content.encode("utf-8")),
    )
    
    # Complete the bundle
    data_pipeline_bus.complete_bundle(
        batch_bid,
        {
            "batch_filename": batch_filename,
            "completed_at": datetime.now(UTC).isoformat(),
        },
    )
    
    # Emit record_loaded events for each record in the batch
    for record in batch_records:
        record_id = record["record_id"]
        snapshot_id = SnapshotId(ocid=record_id["ocid"], bid=record_id["bid"])
        
        data_pipeline_bus.mark_snapshot_as_loaded(
            snapshot_id=snapshot_id,
            metadata={
                "batch_filename": batch_filename,
                "batch_bid": batch_bid,
                "loaded_at": datetime.now(UTC).isoformat(),
            },
        )
    
    logger.info(
        "BATCH_UPLOADED",
        batch_filename=batch_filename,
        batch_bid=batch_bid,
        record_count=len(batch_records),
    )

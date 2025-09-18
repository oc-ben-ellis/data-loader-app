# Data Loader Service

This service implements Phase 9 of the data pipeline - the Loader service. It processes transformed records from the SQS queue and creates batched JSONL files for the legacy ingestion system.

## Overview

The loader service is a batch service triggered by `on_sqs_queue` rules that:

1. Drains up to `batch_max_size` messages from `data-pipeline-load` SQS queue
2. Collates records to JSONL format and stores at `loaded/.../content/batch_<timestamp>-<suffix>.jsonl`
3. Emits `record_loaded` events for each record after upload

## Architecture

The loader follows the same pattern as other pipeline services:

- **Main Entry Point**: `src/data_loader_app/main.py` - CLI interface and orchestration
- **Core Logic**: `src/data_loader_core/loader.py` - Main processing logic
- **Configuration**: `src/data_loader_core/core.py` - Configuration classes
- **Exceptions**: `src/data_loader_core/exceptions.py` - Custom exceptions

## Configuration

The loader uses YAML configuration files. Example configuration:

```yaml
data_source_id: us_fl

loader:
  batch_max_size: 1000000
```

### Configuration Fields

- `batch_max_size`: Maximum number of records to include in a single batch (default: 1,000,000)

## Environment Variables

The loader requires the following environment variables:

- `OC_DATA_PIPELINE_DATA_REGISTRY_ID`: Data registry ID (e.g., "us_fl")
- `OC_DATA_PIPELINE_STAGE`: Pipeline stage (should be "transformed" for loader)
- `OC_DATA_PIPELINE_LOADER_SQS_URL`: URL of the SQS queue containing record_added messages
- `OC_DATA_PIPELINE_STORAGE_S3_URL`: S3 bucket URL for pipeline storage
- `OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL`: SQS queue URL for orchestration events

## Usage

### Command Line

```bash
# Using environment variables (recommended)
export OC_DATA_PIPELINE_DATA_REGISTRY_ID=us_fl
export OC_DATA_PIPELINE_STAGE=transformed
export OC_DATA_PIPELINE_LOADER_SQS_URL=http://sqs-queue-url
python -m data_loader_app.main run

# Using command line arguments
python -m data_loader_app.main run \
  --data-registry-id us_fl \
  --stage transformed \
  --config-dir ./mocks/us_fl/config
```

### Health Check

```bash
python -m data_loader_app.main health --port 8080
```

## Processing Flow

1. **Trigger**: Loader is triggered by orchestration when SQS queue conditions are met
2. **Message Processing**: Reads `record_added` messages from the SQS queue
3. **Record Retrieval**: Fetches transformed record data from S3 using OCID and BID
4. **Batch Creation**: Groups records into JSONL batches up to `batch_max_size`
5. **Upload**: Uploads batch files to `loaded/{data_registry_id}/data/.../content/`
6. **Event Emission**: Emits `record_loaded` events for each processed record

## S3 Storage Layout

Batches are stored in the following S3 structure:

```
s3://bucket/loaded/{data_registry_id}/data/year={year}/month={month}/day={day}/{HH-mm-ss-hex}/
├── metadata/
│   ├── _discovered.json
│   ├── _completed.json
│   ├── _manifest.jsonl
│   └── batch_20250915124912-3b4f1f.jsonl.metadata.json
└── content/
    └── batch_20250915124912-3b4f1f.jsonl
```

## Events

The loader emits the following events to the orchestration queue:

### record_loaded

```json
{
  "event": "record_loaded",
  "data_registry_id": "us_fl",
  "stage": "loaded",
  "record_id": {
    "ocid": "ocid:v1:co:...",
    "bid": "bid:v1:us_fl:..."
  },
  "timestamp": "2025-09-10T03:01:00Z"
}
```

## Error Handling

The loader implements fail-fast error handling:

- **Configuration Errors**: Missing required environment variables or invalid configuration
- **SQS Errors**: Issues reading from or processing SQS messages
- **Storage Errors**: Problems uploading to S3 or retrieving records
- **Batch Errors**: Issues creating or processing batches

All errors are logged with structured logging and cause the service to exit with appropriate error codes.

## Testing

The service includes comprehensive unit and integration tests:

```bash
# Run unit tests
pytest tests/test_unit/

# Run integration tests  
pytest tests/test_integration/

# Run all tests
pytest tests/
```

## Dependencies

- `oc_pipeline_bus`: Pipeline bus library for S3 storage and event emission
- `boto3`: AWS SDK for SQS and S3 operations
- `structlog`: Structured logging
- `pytest`: Testing framework

## Integration with Legacy System

The loader creates JSONL files in the format expected by the legacy OpenCorporates ingestion system. The files are uploaded to the `oc-{environment}-external-ingestion` S3 bucket where the legacy system can pick them up for processing.

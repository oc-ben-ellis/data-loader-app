# US Florida (US_FL) Test Environment Manual Setup Guide

This guide explains how to manually set up and run the US Florida test environment for the data loader application, including how to inspect S3 and SQS outputs.

## Prerequisites

- Docker and Docker Compose
- Terraform
- AWS CLI (for inspecting LocalStack outputs)
- curl (for health checks)
- Poetry (for running the application)

## Important Notes

**Before running this test environment, ensure the following:**

1. **LocalStack Services**: The test environment script has been updated to include `secretsmanager` service. If you encounter "Service 'secretsmanager' is not enabled" errors, ensure you're using the updated `bin/test-env-up.sh` script.

3. **Environment Variable Prefixes**: When using Docker Compose, some environment variables require the `DATA_loader_APP_` prefix:
   - `DATA_loader_APP_STORAGE_USE_UNZIP=false`
   - `DATA_loader_APP_STORAGE_USE_TAR_GZ=false`

4. **Pipeline Bus Compatibility**: If you encounter `DataPipelineBus.__init__() got an unexpected keyword argument '_skip_validation'` errors, ensure the pipeline-bus library is up to date by rebuilding the wheel files.

## Overview

The US_FL test environment consists of:
- **LocalStack**: Mock AWS services (S3, SQS, IAM, CloudWatch)
- **SFTP Server**: Mock SFTP server with test data
- **Data loader App**: The main application container
- **Terraform**: Infrastructure provisioning

**Important**: This guide uses environment variables for configuration, which is the recommended approach. The application will automatically load configuration from the S3 config bucket instead of requiring local config files.

## Step 1: Start the Test Environment

### 1.1 Start LocalStack and Infrastructure

From the project root directory (`/media/ben-ellis/T7/wsldev/oc/oc/data-pipeline-config`):

```bash
# Start LocalStack and provision infrastructure
eval $(./bin/test-env-up.sh --env-vars-only)

# The script will:
# - Start a LocalStack container with S3, SQS, and other AWS services
# - Run Terraform to create S3 buckets and SQS queues
# - Display connection information and environment variables
```

**Important**: Save the output environment variables from the script. You'll need them later:

```bash
# Local AWS/SDK env variables for LocalStack and Context variables for this session :
  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  export AWS_DEFAULT_REGION=eu-west-2
  export AWS_ENDPOINT_URL=http://localhost:4566
  export CONFIG_TEST_ENV_CONTEXT=lsctx-4b
  export CONFIG_TEST_ENV_LOCALSTACK_PORT=4566
  export CONFIG_TEST_ENV_LOCALSTACK_HOST=localhost

# Redis for functional tests (Docker provider):
  export OC_KV_STORE_TYPE=redis
  export OC_KV_STORE_REDIS_HOST=localhost
  export OC_KV_STORE_REDIS_PORT=0

# Data pipeline defaults for mocks/us_fl
  export OC_DATA_PIPELINE_CONFIG_S3_BUCKET=oc-local-data-config
  export OC_DATA_PIPELINE_STORAGE_S3_URL=s3://oc-local-data-pipeline
  export OC_DATA_PIPELINE_STAGE=raw
  export OC_DATA_PIPELINE_STEP=load
  export OC_DATA_PIPELINE_DATA_REGISTRY_ID=us_fl
  export OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL=http://host.docker.internal:4566/000000000000/data-pipeline-orchestration
```

### 1.3 Setup Mock SFTP server

Run the setup script to populate the SFTP server with test data:

```bash
cd mocks/us_fl/environment
docker-compose up -d
./setup-mock-data.sh
cd ../../../
```

This script creates:
- Daily data files: `20230728_daily_data.txt`, `20230729_daily_data.txt`, `20240101_daily_data.txt`
- Quarterly data file: `cordata.zip`
- Proper directory structure: `/doc/cor/` and `/doc/Quarterly/Cor/`

## Step 3: Execute the Application

### 3.1 Upload Configuration to S3

First, upload the configuration files to the S3 config bucket. The application expects configs to be stored in the S3 bucket with the structure: `configs/{data_registry_id}/{step}/`

```bash
# Upload config files to S3 config bucket
aws --endpoint-url ${AWS_ENDPOINT_URL} s3 cp mocks/us_fl/config/config.yaml s3://oc-local-data-config/configs/us_fl/load/config.yaml
aws --endpoint-url ${AWS_ENDPOINT_URL} s3 cp mocks/us_fl/config/sftp_config.yaml s3://oc-local-data-config/configs/us_fl/load/sftp_config.yaml

# Verify the files were uploaded correctly
aws --endpoint-url ${AWS_ENDPOINT_URL} s3 ls s3://oc-local-data-config/configs/us_fl/load/ --recursive
```

### 3.2 Populate AWS Secrets Manager (SFTP credentials)

If you're using AWS Secrets Manager for credentials (recommended for parity with deployed envs), create/update the secret in LocalStack. The loader expects a secret named `{config_name}-sftp-credentials` with keys `host`, `username`, `password`, and `port`. For US_FL the secret name is `us-fl-sftp-credentials`.

```bash

# Create the secret (idempotent: if it exists, update via put-secret-value below)
aws --endpoint-url $AWS_ENDPOINT_URL secretsmanager create-secret \
  --name us-fl-sftp-credentials \
  --secret-string '{"host":"host.docker.internal","username":"test","password":"test","port":"2222"}' \
  || echo "Secret may already exist; updating instead..."

# If the secret already exists, update its value
aws --endpoint-url $AWS_ENDPOINT_URL secretsmanager put-secret-value \
  --secret-id us-fl-sftp-credentials \
  --secret-string '{"host":"host.docker.internal","username":"test","password":"test","port":"2222"}'

# Verify
aws --endpoint-url $AWS_ENDPOINT_URL secretsmanager get-secret-value \
  --secret-id us-fl-sftp-credentials --query SecretString --output text


```

### 3.3 Run the loader Application

```bash
# Build the application container
docker-compose build app-container

# Run the loader with environment variables (no command line arguments needed)
docker-compose run --rm app-container \
  poetry run python -m data_loader_app.main run
```


## Step 4: Inspect Outputs

### 4.1 Inspect S3 Files

List S3 buckets:

```bash
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls
```

The main data bucket should be `oc-local-data-pipeline`. List files in the raw stage:

```bash
# List files in the raw stage for US_FL
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls s3://oc-local-data-pipeline/raw/us_fl/data/ --recursive

# List bundle metadata
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls s3://oc-local-data-pipeline/raw/us_fl/data/ --recursive | grep metadata

# List bundle content
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls s3://oc-local-data-pipeline/raw/us_fl/data/ --recursive | grep content
```

Download and inspect specific files:

```bash
# Download a completed bundle metadata
aws --endpoint-url $AWS_ENDPOINT_URL s3 cp s3://oc-local-data-pipeline/raw/us_fl/data/bundle_*/metadata/_completed.json ./completed.json

# Download the manifest
aws --endpoint-url $AWS_ENDPOINT_URL s3 cp s3://oc-local-data-pipeline/raw/us_fl/data/bundle_*/_manifest.jsonl ./manifest.jsonl

# Download bundle content files
aws --endpoint-url $AWS_ENDPOINT_URL s3 cp s3://oc-local-data-pipeline/raw/us_fl/data/bundle_*/content/ ./downloaded_content/ --recursive

# View file contents
cat completed.json
cat manifest.jsonl
ls -la downloaded_content/
```

### 4.2 Inspect SQS Messages

List SQS queues:

```bash
aws --endpoint-url $AWS_ENDPOINT_URL sqs list-queues
```

Get queue URL and inspect messages:

```bash
# Get queue URL (replace <queue-name> with actual queue name)
QUEUE_URL=$(aws --endpoint-url $AWS_ENDPOINT_URL sqs get-queue-url --queue-name <queue-name> --query 'QueueUrl' --output text)

# Receive messages from the queue
aws --endpoint-url $AWS_ENDPOINT_URL sqs receive-message --queue-url $QUEUE_URL

# Receive and delete messages (to clear the queue)
aws --endpoint-url $AWS_ENDPOINT_URL sqs receive-message --queue-url $QUEUE_URL --max-number-of-messages 10
```

### 4.3 Inspect Local File Outputs

The application creates local files in the `tmp/test_output` directory when using file storage:

```bash
# Check for local output files
find tmp/test_output/ -name "*.csv" -o -name "*.meta" -o -name "*.json" 2>/dev/null

# View the directory structure
tree tmp/test_output/ || ls -la tmp/test_output/

# View bundle metadata files
find tmp/test_output/ -name "_completed.json" -exec cat {} \;

# View manifest files
find tmp/test_output/ -name "_manifest.jsonl" -exec cat {} \;
```

## Step 5: Expected Output Structure

### 5.1 S3 Structure

The application should create files in S3 with this structure:

```
s3://oc-local-data-pipeline/
└── raw/us_fl/data/
    └── bundle_<uuid>/
        ├── _manifest.jsonl
        ├── metadata/
        │   └── _completed.json
        └── content/
            ├── 20230728_daily_data.txt
            ├── 20230729_daily_data.txt
            ├── 20240101_daily_data.txt
            └── cordata.zip
```

Additionally, bundle hashes are stored at:

```
s3://oc-local-data-pipeline/
└── raw/us_fl/bundle_hashes/
    └── _latest
```

### 5.2 SQS Messages

SQS messages should contain:
- Bundle metadata
- File processing notifications
- Error messages (if any)

### 5.3 Bundle Metadata

The `_completed.json` file should contain bundle completion metadata:

```json
{
  "bid": "<bundle-uuid>",
  "status": "completed",
  "created_at": "<timestamp>",
  "data_source_id": "us_fl",
  "stage": "raw",
  "resources_count": 4
}
```

The `_manifest.jsonl` file should contain one JSON line per file:

```json
{"key": "content/20230728_daily_data.txt", "size": 1234, "hash": "sha256:..."}
{"key": "content/20230729_daily_data.txt", "size": 1234, "hash": "sha256:..."}
{"key": "content/20240101_daily_data.txt", "size": 1234, "hash": "sha256:..."}
{"key": "content/cordata.zip", "size": 1234, "hash": "sha256:..."}
```

## Step 6: Troubleshooting

### 6.1 Common Issues

**LocalStack not ready:**
```bash
# Check LocalStack health
curl $AWS_ENDPOINT_URL/health

# Check container logs
docker logs localstack-<context_name>
```

**SFTP connection issues:**
```bash
# Test SFTP connection manually
sftp -P 2222 test@localhost

# Check SFTP container logs
docker-compose logs sftp-server
```

**Application errors:**
```bash
# Check application logs
docker-compose logs app-container

# Run with debug output
export DATA_loader_APP_LOG_LEVEL=DEBUG
poetry run python -m data_loader_app.main run
```

### 6.2 Cleanup

Stop all services:

```bash
# Stop SFTP server
cd mocks/us_fl/environment
docker-compose down

# Stop LocalStack and destroy infrastructure
cd ../../../../../
./bin/test-env-down.sh <context_name> <port>
```

## Step 7: Validation

### 7.1 Verify Data Processing

1. **Check file count**: Should process 4 files (3 daily + 1 quarterly)
2. **Verify bundle structure**: Should create one bundle with all files in the `content/` directory
3. **Check metadata**: Should have `_completed.json` and `_manifest.jsonl` files
4. **Validate bundle hashes**: Should create `_latest` file in `bundle_hashes/` directory
5. **Validate SQS messages**: Should receive processing notifications

### 7.2 Expected File Contents

**Daily files** should contain:
- Mock corporate data
- Company information with licenses
- Proper CSV-like structure

**Quarterly file** should contain:
- Mock quarterly data
- ZIP file format
- Corporate data summary

## Additional Commands

### View All Container Logs
```bash
# LocalStack logs
docker logs localstack-<context_name>

# SFTP server logs
docker-compose logs sftp-server

# Application logs (if running in container)
docker-compose logs app-container
```

### Monitor Real-time
```bash
# Monitor S3 bucket changes
watch -n 5 "aws --endpoint-url $AWS_ENDPOINT_URL s3 ls s3://oc-local-data-pipeline/raw/us_fl/data/ --recursive"

# Monitor SQS queue
watch -n 5 "aws --endpoint-url $AWS_ENDPOINT_URL sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names ApproximateNumberOfMessages"

# Monitor local file storage
watch -n 5 "find tmp/test_output/ -type f | wc -l"
```

This manual setup provides a complete test environment for the US_FL data loader configuration, allowing you to verify end-to-end functionality and inspect all outputs.

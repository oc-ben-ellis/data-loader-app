#!/usr/bin/env bash
set -euo pipefail

# Flag parsing
ENV_VARS_ONLY=0
POSITIONAL_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --env-vars-only)
      ENV_VARS_ONLY=1
      ;;
    --help|-h)
      echo "Usage: test-env-up.sh [--env-vars-only] [context_name] [port]" >&2
      exit 0
      ;;
    *)
      POSITIONAL_ARGS+=("$arg")
      ;;
  esac
done

# Check if --env-vars-only flag is provided
if [ "$ENV_VARS_ONLY" -ne 1 ]; then
  echo "This script should be called with the --env-vars-only flag." >&2
  echo "Please use: eval \$(./bin/test-env-up.sh --env-vars-only)" >&2
  exit 1
fi

# If env-vars-only is requested, redirect normal stdout to stderr so only the
# final export lines (explicitly sent to FD 3) appear on stdout for eval.
if [ "$ENV_VARS_ONLY" -eq 1 ]; then
  exec 3>&1
  exec 1>&2
fi

# Usage: test-env-up.sh [context_name] [port]
# - context_name: optional; if not provided, generated as <random>
# - port: optional; if not provided, first available port starting at 4566 is used

if ! command -v docker &>/dev/null; then
  echo "docker is required" >&2
  exit 1
fi

if ! command -v curl &>/dev/null; then
  echo "curl is required" >&2
  exit 1
fi

if ! command -v terraform &>/dev/null; then
  echo "terraform is required to run init/apply" >&2
  exit 1
fi

generate_context() {
  echo "$(openssl rand -hex 4)"
}

is_port_free() {
  local port="$1"
  if command -v ss &>/dev/null; then
    ! ss -ltn | awk '{print $4}' | grep -q ":${port}$"
  else
    ! netstat -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${port}$"
  fi
}

find_free_port() {
  local start=${1:-4566}
  local end=${2:-4999}
  for ((p=start; p<=end; p++)); do
    if is_port_free "$p"; then
      echo "$p"; return 0
    fi
  done
  echo ""; return 1
}

# Positional args after flag parsing
CTX_NAME=${POSITIONAL_ARGS[0]:-}
PORT_ARG=${POSITIONAL_ARGS[1]:-}

if [ -z "$CTX_NAME" ]; then
  CTX_NAME=$(generate_context)
fi

if [ -z "$PORT_ARG" ]; then
  PORT=$(find_free_port 4566 4999)
  if [ -z "$PORT" ]; then
    echo "Could not find a free port between 4566-4999" >&2
    exit 1
  fi
else
  PORT="$PORT_ARG"
  if ! is_port_free "$PORT"; then
    echo "Port $PORT is not available" >&2
    exit 1
  fi
fi

CONTAINER_NAME="localstack-${CTX_NAME}"

# Clean up any existing container with the same name
echo "Cleaning up any existing container with name ${CONTAINER_NAME}..."
docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
docker rm "${CONTAINER_NAME}" >/dev/null 2>&1 || true

# Detect if running inside a container and determine host address for reaching LocalStack
IN_CONTAINER=0
if [ -f "/.dockerenv" ] || grep -qa "docker\|containerd\|kubepods" /proc/1/cgroup 2>/dev/null; then
  IN_CONTAINER=1
fi

HOST_ADDR="localhost"
if [ "$IN_CONTAINER" -eq 1 ]; then
  # Try host.docker.internal first (works on Docker >=20.10 with host-gateway)
  if getent hosts host.docker.internal >/dev/null 2>&1; then
    HOST_ADDR="host.docker.internal"
  else
    # Fall back to default gateway IP inside container
    GW=$(ip -4 route show default 2>/dev/null | awk '{print $3}' | head -n1)
    if [ -n "$GW" ]; then
      HOST_ADDR="$GW"
    fi
  fi
fi

echo "Starting LocalStack container ${CONTAINER_NAME} on port ${PORT}..."
if ! docker run -d --rm \
  --name "${CONTAINER_NAME}" \
  -p "${PORT}:4566" \
  -e SERVICES=s3,sqs,logs,iam,cloudwatch,cloudformation,sts,secretsmanager \
  -e LS_LOG=info \
  -e DEBUG=0 \
  -v "/var/run/docker.sock:/var/run/docker.sock" \
  localstack/localstack:latest >/dev/null; then
  echo "Failed to start LocalStack container. Check Docker is running and you have permissions." >&2
  exit 1
fi

# Small grace period before health checks
sleep 2

# Wait for LocalStack to be ready (up to ~120s)
printf "Waiting for LocalStack to be ready"
READY=0
RESP=""

health_ok() {
  local url=$1
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" "$url" || echo 000)
  RESP=$(curl -s "$url" || echo "")
  # Consider ready if HTTP 200 and any of these are true:
  # 1) initialized:true
  # 2) s3 and sqs services are "running" or "available"
  # 3) any "running" or "available" appears (fallback for differing shapes)
  if [ "$code" = "200" ]; then
    if echo "$RESP" | grep -q '"initialized"[[:space:]]*:[[:space:]]*true'; then
      return 0
    fi
    if { echo "$RESP" | grep -q '"s3"[[:space:]]*:[[:space:]]*"running"' || echo "$RESP" | grep -q '"s3"[[:space:]]*:[[:space:]]*"available"'; } \
       && { echo "$RESP" | grep -q '"sqs"[[:space:]]*:[[:space:]]*"running"' || echo "$RESP" | grep -q '"sqs"[[:space:]]*:[[:space:]]*"available"'; }; then
      return 0
    fi
    if echo "$RESP" | grep -q '"running"' || echo "$RESP" | grep -q '"available"'; then
      return 0
    fi
  fi
  return 1
}

for ((i=1; i<=120; i++)); do
  if health_ok "http://${HOST_ADDR}:${PORT}/health"; then
    READY=1
  else
    # Try alternate endpoint used by some versions
    if health_ok "http://${HOST_ADDR}:${PORT}/_localstack/health"; then
      READY=1
    fi
  fi

  if [ "$READY" -eq 1 ]; then
    echo ": ready"
    break
  fi

  printf "."
  sleep 1

done

if [ "$READY" -ne 1 ]; then
  echo "" >&2
  echo "LocalStack did not become ready in time" >&2
  echo "Last health response:" >&2
  echo "$RESP" >&2
  exit 1
fi

# Export AWS env vars for LocalStack (applies to terraform commands below)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=eu-west-2
# Note: Terraform AWS provider does not honor a global AWS_ENDPOINT_URL; endpoints must be
# configured in provider settings to fully target LocalStack (s3/sqs/etc.).

cat <<'INFO'
Applying mocks/infra Terraform then service infra/terraform ...
- Using env=local
- WARNING: Ensure provider endpoints are configured for LocalStack.
INFO

# First, attempt to apply mocks/infra to provision shared resources (best-effort)
(
  cd "$(dirname "$0")/../mocks/infra/terraform" 2>/dev/null || exit 0
  rm -rf .terraform .terraform.lock.hcl || true
  if ! terraform init -upgrade -reconfigure; then
    echo "WARNING: Skipping mocks/infra terraform (init failed)" >&2
    exit 0
  fi
  if ! terraform apply -auto-approve -var env=local -var aws_use_localstack=true -var aws_region=eu-west-2 -var "aws_localstack_endpoint=http://${HOST_ADDR}:${PORT}"; then
    echo "WARNING: Skipping mocks/infra terraform (apply failed)" >&2
    exit 0
  fi
)

# Then, apply service-specific infra
(
  cd "$(dirname "$0")/../infra/terraform"
  rm -rf .terraform .terraform.lock.hcl || true
  terraform init -upgrade -reconfigure
  terraform apply -auto-approve -var env=local -var aws_use_localstack=true -var aws_region=eu-west-2 -var "aws_localstack_endpoint=http://${HOST_ADDR}:${PORT}"
)

# Read Redis connection details from terraform outputs (random host port assigned)
TF_DIR="$(dirname "$0")/../infra/terraform"
REDIS_HOST=$(terraform -chdir "$TF_DIR" output -raw redis_host 2>/dev/null || echo "localhost")
REDIS_PORT=$(terraform -chdir "$TF_DIR" output -raw redis_port 2>/dev/null || echo "0")

# Fallback: if Terraform reports 0, query Docker directly for the mapped port
if [ "${REDIS_PORT}" = "0" ] || [ -z "${REDIS_PORT}" ]; then
  if command -v docker &>/dev/null; then
    PORT_FROM_DOCKER=$(docker inspect -f '{{ (index (index .NetworkSettings.Ports "6379/tcp") 0).HostPort }}' loader-functional-redis 2>/dev/null || true)
    if [ -n "${PORT_FROM_DOCKER}" ]; then
      REDIS_PORT="${PORT_FROM_DOCKER}"
    fi
  fi
fi

# Read region and mock outputs (if present) for data pipeline envs
MOCKS_TF_DIR="$(dirname "$0")/../mocks/infra/terraform"
AWS_REGION=$(terraform -chdir "$TF_DIR" output -raw aws_region_out 2>/dev/null || echo "eu-west-2")
PIPELINE_BUCKET=$(terraform -chdir "$MOCKS_TF_DIR" output -raw pipeline_bucket_name 2>/dev/null || echo "oc-local-data-pipeline")
CONFIG_BUCKET=$(terraform -chdir "$MOCKS_TF_DIR" output -raw config_bucket_name 2>/dev/null || echo "oc-local-data-config")
DATA_REGISTRY_ID=$(terraform -chdir "$MOCKS_TF_DIR" output -raw data_registry_id 2>/dev/null || echo "us_fl")
ORCH_SQS_URL="${AWS_ENDPOINT_URL:-http://${HOST_ADDR}:${PORT}}/000000000000/data-pipeline-orchestration"

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=${AWS_REGION}
export AWS_ENDPOINT_URL=http://${HOST_ADDR}:${PORT}
export CONFIG_TEST_ENV_CONTEXT=${CTX_NAME}
export CONFIG_TEST_ENV_LOCALSTACK_PORT=${PORT}
export CONFIG_TEST_ENV_LOCALSTACK_HOST=${HOST_ADDR}
export OC_KV_STORE_TYPE=redis
export OC_KV_STORE_REDIS_HOST=${REDIS_HOST}
export OC_KV_STORE_REDIS_PORT=${REDIS_PORT}
export OC_DATA_PIPELINE_CONFIG_S3_BUCKET=${CONFIG_BUCKET}
export OC_DATA_PIPELINE_STORAGE_S3_URL=${PIPELINE_BUCKET}
export OC_DATA_PIPELINE_STAGE=loaded
export OC_DATA_PIPELINE_STEP=load
export OC_DATA_PIPELINE_DATA_REGISTRY_ID=${DATA_REGISTRY_ID}
export OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL=${ORCH_SQS_URL}

# If only env vars are requested, output them in export format and exit
if [ "$ENV_VARS_ONLY" -eq 1 ]; then
  {
    echo "export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
    echo "export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
    echo "export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}"
    echo "export AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL}"
    echo "export CONFIG_TEST_ENV_CONTEXT=${CONFIG_TEST_ENV_CONTEXT}"
    echo "export CONFIG_TEST_ENV_LOCALSTACK_PORT=${CONFIG_TEST_ENV_LOCALSTACK_PORT}"
    echo "export CONFIG_TEST_ENV_LOCALSTACK_HOST=${HOST_ADDR}"
    echo "export OC_KV_STORE_TYPE=${OC_KV_STORE_TYPE}"
    echo "export OC_KV_STORE_REDIS_HOST=${OC_KV_STORE_REDIS_HOST}"
    echo "export OC_KV_STORE_REDIS_PORT=${REDIS_PORT}"
    echo "export OC_DATA_PIPELINE_CONFIG_S3_BUCKET=${OC_DATA_PIPELINE_CONFIG_S3_BUCKET}"
    echo "export OC_DATA_PIPELINE_STORAGE_S3_URL=s3://${OC_DATA_PIPELINE_STORAGE_S3_URL}"
    echo "export OC_DATA_PIPELINE_STAGE=${OC_DATA_PIPELINE_STAGE}"
    echo "export OC_DATA_PIPELINE_STEP=${OC_DATA_PIPELINE_STEP}"
    echo "export OC_DATA_PIPELINE_DATA_REGISTRY_ID=${OC_DATA_PIPELINE_DATA_REGISTRY_ID}"
    echo "export OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL=${OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL}"
  } >&3
  exit 0
fi

cat <<HINTS

# Local AWS/SDK env variables for LocalStack and Context variables for this session :
  export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
  export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
  export AWS_DEFAULT_REGION=${AWS_REGION}
  export AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL}
  export CONFIG_TEST_ENV_CONTEXT=${CONFIG_TEST_ENV_CONTEXT}
  export CONFIG_TEST_ENV_LOCALSTACK_PORT=${CONFIG_TEST_ENV_LOCALSTACK_PORT}
  export CONFIG_TEST_ENV_LOCALSTACK_HOST=${CONFIG_TEST_ENV_LOCALSTACK_HOST}

# Redis for functional tests (Docker provider):
  export OC_KV_STORE_TYPE=${OC_KV_STORE_TYPE}
  export OC_KV_STORE_REDIS_HOST=${OC_KV_STORE_REDIS_HOST}
  export OC_KV_STORE_REDIS_PORT=${OC_KV_STORE_REDIS_PORT}

# Data pipeline defaults for mocks/us_fl
  export OC_DATA_PIPELINE_CONFIG_S3_BUCKET=${OC_DATA_PIPELINE_CONFIG_S3_BUCKET}
  export OC_DATA_PIPELINE_STORAGE_S3_URL=s3://${OC_DATA_PIPELINE_STORAGE_S3_URL}
  export OC_DATA_PIPELINE_STAGE=${OC_DATA_PIPELINE_STAGE}
  export OC_DATA_PIPELINE_STEP=${OC_DATA_PIPELINE_STEP}
  export OC_DATA_PIPELINE_DATA_REGISTRY_ID=${OC_DATA_PIPELINE_DATA_REGISTRY_ID}
  export OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL=${OC_DATA_PIPELINE_ORCHESTRATION_SQS_URL}

AWS CLI examples (against LocalStack):
  aws --endpoint-url ${AWS_ENDPOINT_URL} s3 ls
  aws --endpoint-url ${AWS_ENDPOINT_URL} sqs list-queues

Redis examples:
  # Test connectivity
  redis-cli -h ${OC_KV_STORE_REDIS_HOST} -p ${OC_KV_STORE_REDIS_PORT} PING

  # Set and get a key
  redis-cli -h ${OC_KV_STORE_REDIS_HOST} -p ${OC_KV_STORE_REDIS_PORT} SET test:ping pong
  redis-cli -h ${OC_KV_STORE_REDIS_HOST} -p ${OC_KV_STORE_REDIS_PORT} GET test:ping

HINTS 1>&2

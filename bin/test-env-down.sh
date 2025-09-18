#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker &>/dev/null; then
  echo "docker is required" >&2
  exit 1
fi

if ! command -v terraform &>/dev/null; then
  echo "terraform is required to run destroy" >&2
  exit 1
fi

# Usage: test-env-down.sh [context_name]
# If context not provided, read from CONFIG_TEST_ENV_CONTEXT
# PORT and HOST are auto-detected from the running container

CTX_NAME=${1:-${CONFIG_TEST_ENV_CONTEXT:-}}

if [ -z "${CTX_NAME}" ]; then
  echo "Missing context. Provide as argument or set environment variable:" >&2
  echo "  export CONFIG_TEST_ENV_CONTEXT=<context>" >&2
  echo "Or call: bash bin/test-env-down.sh <context>" >&2
  exit 1
fi

CONTAINER_NAME="localstack-${CTX_NAME}"

# Check if container exists and get its port mapping
if ! docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
  echo "Container ${CONTAINER_NAME} not found. Nothing to clean up." >&2
  exit 0
fi

# Query the port from the running container
PORT=$(docker port "${CONTAINER_NAME}" 4566/tcp 2>/dev/null | cut -d: -f2 || echo "")
if [ -z "${PORT}" ]; then
  echo "Could not determine port for container ${CONTAINER_NAME}" >&2
  exit 1
fi

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

DATA_DIR="tmp/.localstack"

echo "Stopping LocalStack container ${CONTAINER_NAME}..."
docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true
docker rm "${CONTAINER_NAME}" >/dev/null 2>&1 || true

# Use LocalStack AWS env for terraform destroy
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=eu-west-2

TF_DIR="$(dirname "$0")/../infra/terraform"
if [ -d "$TF_DIR" ]; then
  echo "Destroying Terraform stack (env=local) against endpoint http://${HOST_ADDR}:${PORT} ..."
  (
    cd "$TF_DIR"
    terraform destroy -auto-approve -var env=local -var aws_use_localstack=true -var aws_region=eu-west-2 -var "aws_localstack_endpoint=http://${HOST_ADDR}:${PORT}" || true
  )
fi

echo "Done. To remove persisted data, delete ./${DATA_DIR}"

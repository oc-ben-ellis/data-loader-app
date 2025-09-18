#!/usr/bin/env bash

# Build/Serve TechDocs site inside the TechDocs Docker image without bind mounts.
#
# Usage:
#   bin/techdocs-build-docker.sh [--image IMAGE] [--serve] [--port PORT] [--open] [--verbose] [EXTRA_PATH ...]
#
# Examples:
#   bin/techdocs-build-docker.sh
#   bin/techdocs-build-docker.sh --image roadiehq/techdocs:latest assets stylesheets plugins
#   bin/techdocs-build-docker.sh --serve --port 8001 --open
#   bin/techdocs-build-docker.sh --verbose
#
# Notes:
# - Copies in mkdocs.yaml and docs/ by default. Any EXTRA_PATHs provided
#   will also be copied into the container under /work/ (e.g., assets/, plugins/).
# - Build mode: builds the site at /work/site in the container and copies it back to ./site
# - Serve mode: runs `mkdocs serve` inside the container and maps the port to localhost
# - Requires Docker daemon access.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE="roadiehq/techdocs:latest"
CONTAINER_NAME="techdocs-build"
WORK_DIR="/work"
SITE_LOCAL_DIR="$REPO_ROOT/site"
SERVE=false
PORT=8000
OPEN=false
VERBOSE=false

print_usage() {
  echo "Usage: $0 [--image IMAGE] [--serve] [--port PORT] [--open] [--verbose] [EXTRA_PATH ...]" 2>&1
}

# Parse args
EXTRA_PATHS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      [[ $# -ge 2 ]] || { echo "--image requires a value" 2>&1; exit 2; }
      IMAGE="$2"
      shift 2
      ;;
    --serve)
      SERVE=true
      shift
      ;;
    --port)
      [[ $# -ge 2 ]] || { echo "--port requires a value" 2>&1; exit 2; }
      PORT="$2"
      shift 2
      ;;
    --open)
      OPEN=true
      shift
      ;;
    --verbose)
      VERBOSE=true
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      EXTRA_PATHS+=("$1")
      shift
      ;;
  esac
done

cd "$REPO_ROOT"

# Defensive checks
if [[ ! -f mkdocs.yaml ]]; then
  echo "ERROR: mkdocs.yaml not found at repo root: $REPO_ROOT" 2>&1
  exit 1
fi
if [[ ! -d docs ]]; then
  echo "ERROR: docs/ directory not found at repo root: $REPO_ROOT" 2>&1
  exit 1
fi

if ((${#EXTRA_PATHS[@]} > 0)); then
  for p in "${EXTRA_PATHS[@]}"; do
    if [[ -z "${p}" ]]; then
      echo "ERROR: extra path not found: (empty)" 2>&1
      exit 1
    fi
    if [[ ! -e "$p" ]]; then
      echo "ERROR: extra path not found: $p" 2>&1
      exit 1
    fi
  done
fi

echo "[techdocs] Using image: $IMAGE" 2>&1

# Clean any previous container
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

echo "[techdocs] Creating long-running container..." 2>&1
if $SERVE; then
  # Map port for mkdocs serve
  docker create --name "$CONTAINER_NAME" -p "$PORT:$PORT" --entrypoint sh "$IMAGE" -lc "mkdir -p '$WORK_DIR'; sleep infinity" >/dev/null
else
  docker create --name "$CONTAINER_NAME" --entrypoint sh "$IMAGE" -lc "mkdir -p '$WORK_DIR'; sleep infinity" >/dev/null
fi
docker start "$CONTAINER_NAME" >/dev/null

echo "[techdocs] Copying sources into container..." 2>&1
docker cp mkdocs.yaml           "$CONTAINER_NAME:$WORK_DIR/mkdocs.yaml"
docker cp docs/.                "$CONTAINER_NAME:$WORK_DIR/docs"
if ((${#EXTRA_PATHS[@]} > 0)); then
  for p in "${EXTRA_PATHS[@]}"; do
    base="$(basename "$p")"
    # Copy directories as their contents; files as-is
    if [[ -d "$p" ]]; then
      docker cp "$p/." "$CONTAINER_NAME:$WORK_DIR/$base"
    else
      docker cp "$p" "$CONTAINER_NAME:$WORK_DIR/$base"
    fi
  done
fi

if $SERVE; then
  echo "[techdocs] Serving docs at http://localhost:$PORT ... (Ctrl+C to stop)" 2>&1
  if $OPEN; then
    # Best-effort open in browser
    (xdg-open "http://localhost:$PORT" >/dev/null 2>&1 || true) &
  fi
  # Run mkdocs serve in foreground
  exec docker exec -e PYTHONUNBUFFERED=1 -e PIP_DISABLE_PIP_VERSION_CHECK=1 -it "$CONTAINER_NAME" \
    sh -lc "set -euo pipefail; cd '$WORK_DIR'; mkdocs --version; mkdocs serve -a 0.0.0.0:$PORT"
else
  echo "[techdocs] Building inside container..." 2>&1

  BUILD_CMD="set -euo pipefail; cd '$WORK_DIR'; mkdocs --version; \
    set +e; \
    mkdocs build -v --clean -d '$WORK_DIR/site'; \
    STATUS=\$?; \
    set -e; \
    echo -n \"\$STATUS\" > exit_code;"


  # Run mkdocs and capture status without exiting this script
  set +e
  if $VERBOSE; then
    docker exec -t "$CONTAINER_NAME" sh -lc "$BUILD_CMD";
  else
    docker exec -t "$CONTAINER_NAME" sh -lc "$BUILD_CMD" | grep -E '(ERROR|WARNING|INFO|Aborted)';
  fi
  set -e
  MKDOCS_STATUS_STR=$(docker exec -t "$CONTAINER_NAME" sh -lc "cat \"$WORK_DIR/exit_code\"")

  # Validate and convert to number
  if [[ "$MKDOCS_STATUS_STR" =~ ^[0-9]+$ ]]; then
    MKDOCS_STATUS=$MKDOCS_STATUS_STR
  else
    echo "[techdocs] WARNING: Invalid exit code '$MKDOCS_STATUS_STR', defaulting to 1" 2>&1
    MKDOCS_STATUS=1
  fi

  if [[ $MKDOCS_STATUS -ne 0 ]]; then
    echo "[techdocs] mkdocs build failed with status $MKDOCS_STATUS" 2>&1
    # Verify existence before failing to give a clearer message
    if ! docker exec "$CONTAINER_NAME" sh -lc "test -d '$WORK_DIR/site'"; then
      echo "[techdocs] ERROR: MkDocs did not produce '$WORK_DIR/site'. See build logs above." 2>&1
    fi
    docker rm -f "$CONTAINER_NAME" >/dev/null || true
    exit $MKDOCS_STATUS
  fi


  echo "[techdocs] Verifying site exists in container..." 2>&1
  if ! docker exec "$CONTAINER_NAME" sh -lc "test -d '$WORK_DIR/site'"; then
    echo "[techdocs] ERROR: MkDocs did not produce '$WORK_DIR/site'. See build logs above." 2>&1
    docker rm -f "$CONTAINER_NAME" >/dev/null || true
    exit 1
  fi

  echo "[techdocs] Copying site back to $SITE_LOCAL_DIR ..." 2>&1
  rm -rf "$SITE_LOCAL_DIR"
  docker cp "$CONTAINER_NAME:$WORK_DIR/site" "$SITE_LOCAL_DIR"

  echo "[techdocs] Cleaning up container..." 2>&1
  docker rm -f "$CONTAINER_NAME" >/dev/null || true

  echo "[techdocs] Done. Site available at: $SITE_LOCAL_DIR" 2>&1
fi

"""PyTest configuration and shared test fixtures.

This module provides PyTest configuration, shared fixtures, and test
utilities that are used across multiple test files.
"""

import asyncio
import atexit
import os
import signal
import subprocess
import sys
import tempfile
import uuid
from collections.abc import AsyncGenerator, Coroutine, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import boto3
import pytest
from testcontainers.core.container import (  # type: ignore[import-untyped]
    DockerContainer,
)
from testcontainers.core.waiting_utils import (  # type: ignore[import-untyped]
    wait_for_logs,
)

# Ensure `src` is on the import path for local test runs
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_PATH = _PROJECT_ROOT / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

# Legacy import path shim for modules moved/renamed
import types as _types

"""Pytest configuration and shared fixtures for OC tests."""

# Global flag to track if we're shutting down
_shutdown_requested = False

# Generate a unique test run ID for this session
TEST_RUN_ID = str(uuid.uuid4())[:8]


def cleanup_test_containers() -> None:
    """Stop all containers tagged with the current test run ID."""
    try:
        # Query Docker for containers with our test label
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"label=test-run-id={TEST_RUN_ID}",
                "--format",
                "{{.ID}} {{.Names}}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        if not result.stdout.strip():
            return  # No containers found

        container_lines = result.stdout.strip().split("\n")
        print(
            f"\nCleaning up {len(container_lines)} test containers (run-id: {TEST_RUN_ID})..."
        )

        for line in container_lines:
            if not line.strip():
                continue
            parts = line.split(" ", 1)
            container_id = parts[0]
            container_name = (
                parts[1] if len(parts) > 1 else f"container-{container_id[:12]}"
            )

            try:
                print(f"Stopping container: {container_name} ({container_id[:12]})")
                subprocess.run(
                    ["docker", "stop", container_id], capture_output=True, check=True
                )
                subprocess.run(
                    ["docker", "rm", container_id], capture_output=True, check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"Warning: Failed to stop/remove container {container_name}: {e}")
            except Exception as e:
                print(
                    f"Warning: Unexpected error cleaning up container {container_name}: {e}"
                )

    except subprocess.CalledProcessError as e:
        print(f"Warning: Failed to query Docker containers: {e}")
    except Exception as e:
        print(f"Warning: Unexpected error during container cleanup: {e}")


def add_test_label_to_container(container: DockerContainer) -> None:
    """Add test run label to a container during creation for cleanup tracking."""
    try:
        # Add the test label to the container's labels using with_kwargs
        container.with_kwargs(labels={"test-run-id": TEST_RUN_ID})
        print(f"Container will be tagged with test-run-id: {TEST_RUN_ID}")
    except Exception as e:
        print(f"Warning: Failed to add test label to container: {e}")


def start_container(
    container: DockerContainer, name: str = "container"
) -> DockerContainer:
    """Start a single container and return it."""
    try:
        print(f"Starting {name}...")
        container.start()
        print(f"{name} started successfully")
        return container
    except Exception as e:
        print(f"Failed to start {name}: {e}")
        raise


def stop_container(container: DockerContainer, name: str = "container") -> None:
    """Stop a single container."""
    try:
        print(f"Stopping {name}...")
        container.stop()
        print(f"{name} stopped successfully")
    except Exception as e:
        print(f"Warning: Failed to stop {name}: {e}")


def start_containers_parallel(
    containers: list[tuple[DockerContainer, str]],
) -> list[DockerContainer]:
    """Start multiple containers in parallel."""
    if not containers:
        return []

    print(f"Starting {len(containers)} containers in parallel...")
    started_containers = []

    with ThreadPoolExecutor(max_workers=min(len(containers), 4)) as executor:
        # Submit all container start tasks
        future_to_container = {
            executor.submit(start_container, container, name): (container, name)
            for container, name in containers
        }

        # Collect results as they complete
        for future in as_completed(future_to_container):
            container, name = future_to_container[future]
            try:
                started_container = future.result()
                started_containers.append(started_container)
            except Exception as e:
                print(f"Failed to start {name}: {e}")
                # Stop any containers that were already started
                for started_container in started_containers:
                    try:
                        stop_container(started_container, "failed-container")
                    except:
                        pass
                raise

    print(f"Successfully started {len(started_containers)} containers")
    return started_containers


def stop_containers_parallel(containers: list[tuple[DockerContainer, str]]) -> None:
    """Stop multiple containers in parallel."""
    if not containers:
        return

    print(f"Stopping {len(containers)} containers in parallel...")

    with ThreadPoolExecutor(max_workers=min(len(containers), 4)) as executor:
        # Submit all container stop tasks
        futures = [
            executor.submit(stop_container, container, name)
            for container, name in containers
        ]

        # Wait for all to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Warning: Container stop failed: {e}")

    print(f"Successfully stopped {len(containers)} containers")


def create_parallel_container_fixture(
    container_configs: list[tuple[str, dict[str, Any], str]],
) -> Any:
    """Create a pytest fixture that starts multiple containers in parallel.

    Args:
        container_configs: List of (image, config_dict, name) tuples

    Returns:
        A pytest fixture function
    """

    def _fixture() -> Any:
        # Create containers
        containers = []
        for image, config, name in container_configs:
            container = DockerContainer(image)

            # Apply configuration
            for key, value in config.items():
                if key == "env":
                    for env_key, env_value in value.items():
                        container.with_env(env_key, env_value)
                elif key == "ports":
                    container.with_exposed_ports(*value)
                elif key == "command":
                    container.with_command(value)
                elif key == "labels":
                    container.with_kwargs(labels=value)
                else:
                    # Pass other config as kwargs
                    container.with_kwargs(**{key: value})

            # Add test label
            add_test_label_to_container(container)
            containers.append((container, name))

        # Start containers in parallel
        try:
            start_containers_parallel(containers)
            yield [container for container, _ in containers]
        finally:
            # Stop containers in parallel
            stop_containers_parallel(containers)

    return _fixture


def _signal_handler(signum: int, frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    global _shutdown_requested
    _shutdown_requested = True
    print(f"\nReceived signal {signum}, shutting down gracefully...")

    # Clean up containers before exiting
    cleanup_test_containers()

    # Force exit to bypass pytest's signal handling
    os._exit(130)  # Exit code 130 is standard for SIGINT


# Register atexit handler for cleanup
atexit.register(cleanup_test_containers)


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    # Ensure the newly created loop is the current event loop for the session
    asyncio.set_event_loop(loop)

    # Override the default exception handler to avoid logging errors during shutdown
    def custom_exception_handler(
        loop: asyncio.AbstractEventLoop, context: dict[str, Any]
    ) -> None:
        """Custom exception handler that ignores certain errors during shutdown."""
        global _shutdown_requested

        # Check if we're shutting down and the error is related to closed files/streams
        if _shutdown_requested:
            exception = context.get("exception")
            if exception and isinstance(exception, ValueError | OSError):
                if "I/O operation on closed file" in str(
                    exception
                ) or "closed file" in str(exception):
                    # Silently ignore these errors during shutdown
                    return

        # For other errors, use the default handler
        loop.default_exception_handler(context)

    loop.set_exception_handler(custom_exception_handler)

    yield loop

    # Clean up any remaining tasks before closing the loop
    pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
    if pending_tasks:
        print(f"Cleaning up {len(pending_tasks)} pending tasks...")
        for task in pending_tasks:
            task.cancel()

        # Wait for tasks to be cancelled
        if pending_tasks:
            loop.run_until_complete(
                asyncio.gather(*pending_tasks, return_exceptions=True)
            )

    # Ensure async generators are properly shut down before closing the loop
    shutdown_coro: Coroutine[Any, Any, None] | None = None
    try:
        if not loop.is_closed():
            shutdown_coro = loop.shutdown_asyncgens()
            loop.run_until_complete(shutdown_coro)
    except Exception:
        # If we created the coroutine but could not await it, close it to avoid warnings
        if shutdown_coro is not None:
            shutdown_coro.close()
        # Best-effort shutdown; ignore errors here to avoid masking test results

    # Detach the loop and close it to avoid unclosed loop warnings
    asyncio.set_event_loop(None)
    loop.close()


@pytest.fixture
def temp_dir() -> Generator[str]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir



def create_test_stream(content: bytes) -> AsyncGenerator[bytes]:
    """Create a test stream from bytes."""

    async def stream() -> AsyncGenerator[bytes]:
        yield content

    return stream()


@pytest.fixture
def test_stream_factory() -> Any:
    """Factory for creating test streams."""
    return create_test_stream


@pytest.fixture(scope="class")
def localstack_container() -> DockerContainer:
    """Start localstack container for S3 testing."""
    # Fail if running in CI without Docker
    if os.getenv("CI") and not os.path.exists("/var/run/docker.sock"):
        pytest.fail("Docker not available in CI environment")

    try:
        container = DockerContainer("localstack/localstack:3.0")
        container.with_env("SERVICES", "s3,secretsmanager")
        container.with_env("DEFAULT_REGION", "us-east-1")
        container.with_env("AWS_ACCESS_KEY_ID", "test")
        container.with_env("AWS_SECRET_ACCESS_KEY", "test")
        container.with_env("DEBUG", "1")
        container.with_env("STATE_MANAGEMENT", "1")
        container.with_exposed_ports(4566)

        # Add test label for cleanup tracking (before starting)
        add_test_label_to_container(container)

        container.start()

        # Actively poll the S3 endpoint instead of waiting for specific logs.
        import time

        max_attempts = 60  # up to ~60 seconds
        for attempt in range(max_attempts):
            try:
                host_ip = container.get_container_host_ip()
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=f"http://{host_ip}:{container.get_exposed_port(4566)}",
                    aws_access_key_id="test",
                    aws_secret_access_key="test",
                    region_name="us-east-1",
                )
                s3_client.list_buckets()
                print("LocalStack S3 service is ready")
                break
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep(1.0)
                else:
                    print(
                        f"Warning: LocalStack S3 not ready after {max_attempts} attempts: {e}"
                    )
                    # Proceed and let test decide how to handle connectivity

        yield container

        # Stop container (label cleanup will be handled by Docker query)
        container.stop()
    except Exception as e:
        # Ensure container is stopped even if setup fails
        try:
            container.stop()
        except:
            pass
        pytest.fail(f"Failed to start localstack container: {e}")


@pytest.fixture(scope="class")
def redis_container() -> DockerContainer:
    """Start Redis container for testing."""
    # Fail if running in CI without Docker
    if os.getenv("CI") and not os.path.exists("/var/run/docker.sock"):
        pytest.fail("Docker not available in CI environment")

    try:
        container = DockerContainer("redis:7-alpine")
        container.with_exposed_ports(6379)

        # Add test label for cleanup tracking (before starting)
        add_test_label_to_container(container)

        container.start()

        # Wait for Redis to be ready with optimized health check
        wait_for_logs(container, "Ready to accept connections")

        # Quick health check to ensure Redis is actually responding
        import time

        import redis

        max_attempts = 5  # Reduced attempts for faster startup
        for attempt in range(max_attempts):
            try:
                test_client = redis.Redis(
                    host=container.get_container_host_ip(),
                    port=container.get_exposed_port(6379),
                    db=0,
                    socket_connect_timeout=1,  # Fast timeout
                )
                test_client.ping()
                print("Redis is ready and responding")
                break
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep(0.2)  # Very short wait between attempts
                else:
                    print(f"Warning: Redis health check failed: {e}")
                    # Continue anyway, the test will fail if Redis isn't working

        yield container

        # Stop container (label cleanup will be handled by Docker query)
        container.stop()
    except Exception as e:
        # Ensure container is stopped even if setup fails
        try:
            container.stop()
        except:
            pass
        pytest.fail(f"Failed to start Redis container: {e}")


@pytest.fixture
def s3_client(localstack_container: DockerContainer) -> Any:
    """Create S3 client connected to localstack."""
    # Use container host IP for Docker-in-Docker environments
    host_ip = localstack_container.get_container_host_ip()
    return boto3.client(
        "s3",
        endpoint_url=f"http://{host_ip}:{localstack_container.get_exposed_port(4566)}",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


@pytest.fixture
def secretsmanager_client(localstack_container: DockerContainer) -> Any:
    """Create Secrets Manager client connected to localstack."""
    # Use container host IP for Docker-in-Docker environments
    host_ip = localstack_container.get_container_host_ip()
    return boto3.client(
        "secretsmanager",
        endpoint_url=f"http://{host_ip}:{localstack_container.get_exposed_port(4566)}",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


@pytest.fixture
def test_secrets(secretsmanager_client: Any) -> dict[str, dict[str, str]]:
    """Create test secrets in LocalStack Secrets Manager."""
    import json

    # Create test API credentials for sample configuration
    api_credentials = {
        "api_key": "test_api_key",
        "api_secret": "test_api_secret",
        "base_url": "http://localhost:5000",
    }

    # Create test database credentials for sample configuration
    db_credentials = {
        "host": "localhost",
        "username": "testuser",
        "password": "testpass",
        "port": "5432",
        "database": "testdb",
    }

    # Create API secret
    api_secret_name = "sample-api-credentials"
    try:
        secretsmanager_client.create_secret(
            Name=api_secret_name, SecretString=json.dumps(api_credentials)
        )
    except Exception:
        # Secret might already exist
        pass

    # Create database secret
    db_secret_name = "sample-db-credentials"
    try:
        secretsmanager_client.create_secret(
            Name=db_secret_name, SecretString=json.dumps(db_credentials)
        )
    except Exception:
        # Secret might already exist
        pass

    return {
        "sample-api-credentials": api_credentials,
        "sample-db-credentials": db_credentials,
    }


@pytest.fixture(scope="class")
def parallel_containers() -> Generator[tuple[DockerContainer, DockerContainer]]:
    """Start both localstack and Redis containers in parallel for faster test setup."""
    # Fail if running in CI without Docker
    if os.getenv("CI") and not os.path.exists("/var/run/docker.sock"):
        pytest.fail("Docker not available in CI environment")

    # Create containers
    localstack_container = DockerContainer("localstack/localstack:3.0")
    localstack_container.with_env("SERVICES", "s3,secretsmanager")
    localstack_container.with_env("DEFAULT_REGION", "us-east-1")
    localstack_container.with_env("AWS_ACCESS_KEY_ID", "test")
    localstack_container.with_env("AWS_SECRET_ACCESS_KEY", "test")
    localstack_container.with_env("DEBUG", "1")
    localstack_container.with_env("STATE_MANAGEMENT", "1")
    localstack_container.with_exposed_ports(4566)
    add_test_label_to_container(localstack_container)

    redis_container = DockerContainer("redis:7-alpine")
    redis_container.with_exposed_ports(6379)
    add_test_label_to_container(redis_container)

    # Start containers in parallel
    containers_to_start = [
        (localstack_container, "localstack"),
        (redis_container, "redis"),
    ]

    try:
        start_containers_parallel(containers_to_start)

        # Wait for services to be ready by polling endpoints instead of log matching
        print("Waiting for services to be ready...")
        import time

        # Poll LocalStack Secrets Manager
        host_ip = localstack_container.get_container_host_ip()
        max_attempts = 60
        for attempt in range(max_attempts):
            try:
                test_client = boto3.client(
                    "secretsmanager",
                    endpoint_url=f"http://{host_ip}:{localstack_container.get_exposed_port(4566)}",
                    aws_access_key_id="test",
                    aws_secret_access_key="test",
                    region_name="us-east-1",
                )
                test_client.list_secrets()
                print("Secrets Manager service is ready")
                break
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep(1.0)
                else:
                    print(f"Warning: Secrets Manager not ready: {e}")

        # Poll Redis by attempting a ping
        try:
            import redis

            host_ip_redis = redis_container.get_container_host_ip()
            max_attempts = 30
            for attempt in range(max_attempts):
                try:
                    test_client = redis.Redis(
                        host=host_ip_redis,
                        port=redis_container.get_exposed_port(6379),
                        db=0,
                        socket_connect_timeout=1,
                    )
                    test_client.ping()
                    print("Redis is ready and responding")
                    break
                except Exception:
                    if attempt < max_attempts - 1:
                        time.sleep(1.0)
                    else:
                        print("Warning: Redis not responding to ping")
        except Exception:
            # If redis lib not available, continue; tests will fail if Redis is required
            pass

        yield localstack_container, redis_container

        # Stop containers in parallel
        containers_to_stop = [
            (localstack_container, "localstack"),
            (redis_container, "redis"),
        ]
        stop_containers_parallel(containers_to_stop)

    except Exception as e:
        # Ensure containers are stopped even if setup fails
        try:
            containers_to_stop = [
                (localstack_container, "localstack"),
                (redis_container, "redis"),
            ]
            stop_containers_parallel(containers_to_stop)
        except:
            pass
        pytest.fail(f"Failed to start parallel containers: {e}")


# Mark tests that require Docker/localstack
def pytest_configure(config: Any) -> None:
    """Configure pytest with custom markers and signal handling."""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    config.addinivalue_line(
        "markers", "localstack: mark test as requiring localstack container"
    )
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """Clean up any remaining containers and asyncio tasks at the end of the test session."""
    # Clean up containers first
    cleanup_test_containers()

    # Then clean up asyncio tasks
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            return  # Don't interfere with running loop

        # Cancel any remaining tasks
        pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        if pending_tasks:
            print(f"Cleaning up {len(pending_tasks)} remaining tasks...")
            for task in pending_tasks:
                task.cancel()

            # Wait for tasks to be cancelled
            if pending_tasks:
                loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )
    except Exception:
        # Ignore errors during cleanup
        pass


def pytest_runtest_setup(item: Any) -> None:
    """Ensure setup failures cause test failures instead of skips."""
    # This hook runs before each test and can catch setup issues


def pytest_runtest_teardown(item: Any, nextitem: Any) -> None:
    """Ensure teardown failures are properly reported."""
    # This hook runs after each test and can catch teardown issues


# Fail localstack tests if Docker is not available
def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    """Modify test collection to fail localstack tests if Docker is not available."""
    for item in items:
        if "localstack" in item.keywords:
            if os.getenv("CI") and not os.path.exists("/var/run/docker.sock"):
                # Mark the test to fail during setup if Docker is not available
                item.add_marker(
                    pytest.mark.xfail(
                        reason="Docker/localstack not available in CI environment",
                        strict=True,
                    )
                )

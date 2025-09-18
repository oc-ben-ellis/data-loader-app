"""Application configuration module (CLI + factories).

This module merges the CLI configuration (argument/env parsing) and the
factory functions to build the application's dependencies into a single
cohesive module.
"""

from typing import TypedDict, Unpack, cast

import environ
from openc_python_common.envargs import args_to_config_class

from data_loader_core.config import loaderConfig
from data_loader_core.credentials import (
    create_credential_provider,
)
from data_loader_core.kv_store import (
    create_kv_store,
)
from data_loader_core.storage import Storage, create_storage_config_instance


class StorageCreationError(Exception):
    """Raised when storage creation fails."""

    def __init__(self, storage_type: str) -> None:
        """Initialize the error with storage type information.

        Args:
            storage_type: The type of storage that failed to create.
        """
        super().__init__(f"Failed to create storage of type: {storage_type}")
        self.storage_type = storage_type


class ConfigKwargs(TypedDict, total=False):
    """Type definition for configuration kwargs."""

    # AWS credential provider kwargs
    aws_region: str
    aws_endpoint_url: str
    # Environment credential provider kwargs
    env_prefix: str
    # Redis store kwargs
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: str
    # Storage kwargs
    s3_bucket: str
    s3_prefix: str
    s3_region: str
    s3_endpoint_url: str
    file_path: str
    use_unzip: bool
    # Common kwargs
    serializer: str
    default_ttl: int
    data_registry_id: str


async def create_loader_app_config(
    credentials_provider_type: str | None = None,
    storage_type: str | None = None,
    kv_store_type: str | None = None,
    **kwargs: Unpack[ConfigKwargs],
) -> loaderConfig:
    """Create a complete loader configuration."""

    def _with_prefixes(src: dict[str, object], *prefixes: str) -> dict[str, object]:
        return {k: v for k, v in src.items() if k.startswith(prefixes)}

    # Create credential provider
    credential_provider = create_credential_provider(
        provider_type=credentials_provider_type,
        **_with_prefixes(kwargs, "aws_", "env_"),  # type: ignore[arg-type]
    )

    # Create key-value store
    kv_store = create_kv_store(
        store_type=kv_store_type,
        **_with_prefixes(kwargs, "redis_", "serializer", "default_ttl", "config_id"),  # type: ignore[arg-type]
    )

    # Create storage instance
    storage_config = create_storage_config_instance(
        storage_type=storage_type,
        **_with_prefixes(kwargs, "s3_", "file_", "use_"),  # type: ignore[arg-type]
    )

    # Build the actual storage instance
    storage = cast("Storage", storage_config.build())
    if not storage:
        raise StorageCreationError(storage_type or "unknown")

    return loaderConfig(
        credential_provider=credential_provider,
        kv_store=kv_store,
        storage=storage,
    )


@environ.config(prefix="DATA_loader_APP")
class RunConfig:
    """Configuration for the run command."""

    data_registry_id: str | None = environ.var(
        default=None,
        help="Data registry ID (e.g., us_fl). If not specified, uses environment variables.",
    )
    stage: str | None = environ.var(
        default=None,
        help="Pipeline stage (e.g., loader). Required if --data-registry-id is specified.",
    )
    step: str | None = environ.var(
        default=None,
        help="Pipeline step used for config selection (e.g., loader). "
        "Required when --data-registry-id is specified.",
    )
    credentials_provider: str = environ.var(
        default="aws", help="Credential provider to use (aws or env)"
    )
    storage: str = environ.var(
        default="pipeline", help="Storage mechanism to use (pipeline, s3 or file)"
    )
    kvstore: str = environ.var(
        default="redis", help="Key-value store to use (redis or memory)"
    )

    # Global AWS profile default (applies to credentials and storage unless overridden)
    aws_profile: str | None = environ.var(
        default=None, help="Default AWS profile to use for AWS SDK clients"
    )

    # Credentials provider configuration
    credentials_aws_region: str | None = environ.var(
        default=None,
        help="AWS region for credential provider (when using aws)",
    )
    credentials_aws_endpoint_url: str | None = environ.var(
        default=None,
        help="AWS endpoint URL for credential provider (e.g., LocalStack)",
    )
    credentials_env_prefix: str | None = environ.var(
        default=None,
        help="Environment variable prefix for environment credential provider",
    )

    # KV store configuration
    kvstore_serializer: str | None = environ.var(
        default=None,
        help="Serializer to use for KV store (json or pickle)",
    )
    kvstore_default_ttl: int | None = environ.var(
        default=None,
        help="Default TTL (seconds) for KV store entries",
    )
    kvstore_redis_host: str | None = environ.var(
        default=None,
        help="Redis host for KV store (when using redis)",
    )
    kvstore_redis_port: int | None = environ.var(
        default=None,
        help="Redis port for KV store (when using redis)",
    )
    kvstore_redis_db: int | None = environ.var(
        default=None,
        help="Redis database number for KV store (when using redis)",
    )
    kvstore_redis_password: str | None = environ.var(
        default=None,
        help="Redis password for KV store (when using redis)",
    )
    kvstore_redis_key_prefix: str | None = environ.var(
        default=None,
        help="Redis key prefix for KV store (when using redis)",
    )

    # Storage configuration
    storage_pipeline_aws_profile: str | None = environ.var(
        default=None,
        help="AWS profile override for pipeline storage related AWS clients",
    )
    storage_s3_bucket: str | None = environ.var(
        default=None, help="S3 bucket name (when using s3 storage)"
    )
    storage_s3_prefix: str | None = environ.var(
        default=None, help="S3 key prefix (when using s3 storage)"
    )
    storage_s3_region: str | None = environ.var(
        default=None, help="S3 region (when using s3 storage)"
    )
    storage_s3_endpoint_url: str | None = environ.var(
        default=None, help="S3 endpoint URL (e.g., LocalStack)"
    )
    storage_file_path: str | None = environ.var(
        default=None, help="Local file storage base path (when using file storage)"
    )
    storage_use_unzip: bool | None = environ.bool_var(
        default=True, help="Enable unzip decorator for storage operations"
    )
    storage_use_tar_gz: bool | None = environ.bool_var(
        default=True, help="Enable tar/gz decorator for storage operations"
    )
    log_level: str = environ.var(default="INFO", help="Log level")
    dev_mode: bool = environ.bool_var(
        default=False, help="Enable development mode logging"
    )

    # Credentials provider AWS profile override
    credentials_aws_profile: str | None = environ.var(
        default=None,
        help="AWS profile override for credential provider AWS SDK clients",
    )

    # YAML Configuration
    config_dir: str | None = environ.var(
        default=None,
        help="Local directory containing YAML configuration files (overrides S3 config loading)",
    )


@environ.config(prefix="DATA_loader_APP")
class HealthConfig:
    """Configuration for the health check command."""

    port: int = environ.var(default=8080, help="Port to bind to")
    host: str = environ.var(default="127.0.0.1", help="Host to bind to")
    log_level: str = environ.var(default="INFO", help="Log level")
    dev_mode: bool = environ.bool_var(
        default=False, help="Enable development mode logging"
    )


def create_run_config(args: list[str] | None = None) -> RunConfig:
    """Create a RunConfig from command line arguments and environment variables."""
    return args_to_config_class(RunConfig, args)


def create_health_config(args: list[str] | None = None) -> HealthConfig:
    """Create a HealthConfig from command line arguments and environment variables."""
    return args_to_config_class(HealthConfig, args)


# Backward compatibility aliases
AppConfig = loaderConfig
create_app_config = create_loader_app_config

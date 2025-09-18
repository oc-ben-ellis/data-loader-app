"""Command-line interface and main entry point.

This module provides the main CLI interface for running loaders, including
argument parsing, configuration loading, and execution orchestration.
"""
# ruff: noqa: T201

import asyncio
import os
import sys
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from wsgiref.simple_server import make_server

import structlog

# YAML configuration support
from oc_pipeline_bus.config import DataPipelineConfig
from openc_python_common.observability import (
    log_bind,
    observe_around,
)

from data_loader_app.app_config import (
    loaderConfig,
    create_loader_app_config,
    create_health_config,
    create_run_config,
)
from data_loader_app.health import create_health_app
from data_loader_core.logging import (
    ConsoleMode,
    LoggingHandler,
    LoggingLevel,
    configure_logging,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from wsgiref.types import StartResponse

# Application configuration name
config_name = os.getenv("OC_CONFIG_ID")


def configure_application_credential_provider(
    _loader: loader, _app_config: loaderConfig
) -> None:
    """Deprecated: credential provider is injected via app_config. No-op."""
    return


# Get logger for this module
logger = structlog.get_logger(__name__)


def generate_run_id(config_id: str) -> str:
    """Generate a unique run ID combining config_id and timestamp.

    Args:
        config_id: The configuration identifier.

    Returns:
        A unique run ID in the format: loader_{config_id}_{timestamp}
    """
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S")
    return f"loader_{config_id}_{timestamp}"


# ruff: noqa: PLR0912
def run_command(args: list[str] | None = None) -> None:
    """Run a data loader with the specified configuration.

    This command executes a data loader using the provided configuration ID.
    The loader will process data according to its configured rules and output
    the results to the specified destination.

    Args:
        args: Command line arguments. If None, uses sys.argv.
    """

    def _raise_data_registry_id_required() -> None:
        """Raise ValueError for missing data_registry_id."""
        msg = "data_registry_id is required. Either specify --data-registry-id or set OC_DATA_PIPELINE_DATA_REGISTRY_ID environment variable"
        print(f"Error: {msg}")
        raise ValueError(msg)

    try:
        # Parse command line arguments
        config = create_run_config(args)

        # Validate configuration
        if config.data_registry_id is not None and config.stage is None:
            print("Error: --stage is required when --data-registry-id is specified")
            sys.exit(1)

        if config.data_registry_id is not None and config.step is None:
            print("Error: --step is required when --data-registry-id is specified")
            sys.exit(1)

        if (
            config.data_registry_id is not None
            or config.stage is not None
            or config.step is not None
        ) and config.config_dir is None:
            print(
                "Error: --config-dir is required when --data-registry-id or --stage or --step is specified"
            )
            sys.exit(1)

        # Determine the data registry ID to use
        data_registry_id = config.data_registry_id
        if data_registry_id is None:
            # Try to get from environment variable
            data_registry_id = os.environ.get("OC_DATA_PIPELINE_DATA_REGISTRY_ID")
            if not data_registry_id:
                _raise_data_registry_id_required()

        # Set environment variables for pipeline config if specified
        if config.data_registry_id is not None:
            os.environ["OC_DATA_PIPELINE_DATA_REGISTRY_ID"] = config.data_registry_id
        if config.stage is not None:
            os.environ["OC_DATA_PIPELINE_STAGE"] = config.stage
        if config.step is not None:
            os.environ["OC_DATA_PIPELINE_STEP"] = config.step

        # Generate run_id
        run_id = generate_run_id(str(data_registry_id))

        # Configure logging
        configure_logging(
            logging_level=LoggingLevel(config.log_level.upper()),
            package_log_levels={},
            logging_handler=LoggingHandler.TEXT,
            console_mode=ConsoleMode.FORCE if config.dev_mode else ConsoleMode.AUTO,
        )

        # Map CLI config fields to env/factory kwargs, only including provided values
        factory_kwargs: dict[str, Any] = {}

        # Environment overrides
        env_overrides = {
            "aws_profile": "AWS_PROFILE",
            "storage_pipeline_aws_profile": "OC_STORAGE_PIPELINE_AWS_PROFILE",
            "credentials_aws_profile": "OC_CREDENTIAL_PROVIDER_AWS_PROFILE",
        }
        for field, env_name in env_overrides.items():
            value = getattr(config, field, None)
            if value is not None:
                os.environ[env_name] = value

        # Factory kwargs mapping
        field_map = {
            # credentials
            "credentials_aws_region": "aws_region",
            "credentials_aws_endpoint_url": "aws_endpoint_url",
            "credentials_env_prefix": "env_prefix",
            # kvstore
            "kvstore_serializer": "serializer",
            "kvstore_default_ttl": "default_ttl",
            "kvstore_redis_host": "redis_host",
            "kvstore_redis_port": "redis_port",
            "kvstore_redis_db": "redis_db",
            "kvstore_redis_password": "redis_password",
            "kvstore_redis_key_prefix": "redis_key_prefix",
            # storage
            "storage_s3_bucket": "s3_bucket",
            "storage_s3_prefix": "s3_prefix",
            "storage_s3_region": "s3_region",
            "storage_s3_endpoint_url": "s3_endpoint_url",
            "storage_file_path": "file_path",
            "storage_use_unzip": "use_unzip",
            "storage_use_tar_gz": "use_tar_gz",
        }
        for src, dst in field_map.items():
            val = getattr(config, src, None)
            if val is not None:
                factory_kwargs[dst] = val

        # Store the arguments for the async main function
        args_dict = {
            "config_name": data_registry_id,
            "credentials_provider": config.credentials_provider,
            "storage": config.storage,
            "kvstore": config.kvstore,
            "run_id": run_id,
            "factory_kwargs": factory_kwargs,
            "config_dir": config.config_dir,
            "data_registry_id": data_registry_id,
            "stage": config.stage,
            "step": config.step,
        }

        # Run the async main function with robust error handling
        try:
            asyncio.run(main_async(args_dict))
        except KeyboardInterrupt:
            logger.info("RUN_CANCELLED_BY_USER")
            sys.exit(130)
        except KeyError as e:
            # Config-related errors (e.g., missing strategy IDs)
            logger.exception("RUN_COMMAND_CONFIG_ERROR", error=str(e))
            sys.exit(2)
        except Exception as e:
            # Catch-all to ensure errors are logged and correct exit code returned
            logger.exception("RUN_COMMAND_ERROR", error=str(e))
            sys.exit(1)

    except Exception as e:
        # Fail-fast for setup/argument parsing issues
        logger.exception("RUN_STARTUP_ERROR", error=str(e))
        sys.exit(1)


def health_command(args: list[str] | None = None) -> None:
    """Start a health check server.

    This command starts a WSGI server with health check endpoints.

    Args:
        args: Command line arguments. If None, uses sys.argv.
    """
    try:
        config = create_health_config(args)

        # Configure logging
        configure_logging(
            logging_level=LoggingLevel(config.log_level.upper()),
            package_log_levels={},
            logging_handler=LoggingHandler.TEXT,
            console_mode=ConsoleMode.FORCE if config.dev_mode else ConsoleMode.AUTO,
        )

        # Create health check app
        app = create_health_app()

        # Import wsgiref for simple WSGI server

        logger.info("HEALTH_CHECK_SERVER_STARTING", host=config.host, port=config.port)

        with make_server(
            config.host,
            config.port,
            cast("Callable[[dict[str, Any], StartResponse], Any]", app),
        ) as httpd:
            logger.info(
                "HEALTH_CHECK_SERVER_STARTED",
                host=config.host,
                port=config.port,
                endpoints=["/health", "/status", "/heartbeat"],
            )
            httpd.serve_forever()

    except KeyboardInterrupt:
        logger.info("HEALTH_CHECK_SERVER_STOPPED_BY_USER")
    except Exception as e:
        print(f"Error: {e!s}")
        logger.exception("HEALTH_CHECK_SERVER_START_ERROR", error=str(e))
        sys.exit(1)


def show_help() -> None:
    """Show help information for the CLI."""
    help_text = """
OpenCorporates Data loader

Usage:
    python -m data_loader_app.main <command> [options]

Commands:
    run                     Run a data loader (uses environment variables for config)
    health                  Start a health check server
    --help, -h              Show this help message
    --version, -v           Show version information

Options for run command:
    --data-registry-id <id>     Data registry ID (e.g., us_fl). If not specified, uses OC_DATA_PIPELINE_DATA_REGISTRY_ID env var
    --stage <stage>             Pipeline stage (e.g., raw). Required if --data-registry-id is specified
    --config-dir <path>         Local directory containing YAML configuration files. Required if --data-registry-id or --stage is specified
    --credentials-provider <type>  Credential provider type (aws, env)
    --storage <type>              Storage type (s3, file)
    --kvstore <type>              Key-value store type (redis, memory)
    --log-level <level>           Log level (DEBUG, INFO, WARNING, ERROR)
    --dev-mode                    Enable development mode

Examples:
    # Using environment variables (recommended)
    export OC_DATA_PIPELINE_DATA_REGISTRY_ID=us_fl
    export OC_DATA_PIPELINE_STAGE=raw
    python -m data_loader_app.main run

    # Using command line arguments (all three required together)
    python -m data_loader_app.main run --data-registry-id us-fl --stage raw --config-dir ./mocks/us_fl/config
    python -m data_loader_app.main run --data-registry-id us-fl --stage raw --config-dir ./mocks/us_fl/config --storage file
    python -m data_loader_app.main health --port 8080
"""
    print(help_text)


def main() -> None:
    """Main entry point for the CLI."""
    min_args = 2
    if len(sys.argv) < min_args:
        show_help()
        sys.exit(1)

    command = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > min_args else []

    dispatch = {
        "run": lambda: run_command(args),
        "health": lambda: health_command(args),
        "--help": show_help,
        "-h": show_help,
        "help": show_help,
        "--version": lambda: print("data-loader-app, version 0.1.0"),
        "-v": lambda: print("data-loader-app, version 0.1.0"),
        "version": lambda: print("data-loader-app, version 0.1.0"),
    }
    handler = dispatch.get(command)
    if handler is None:
        show_help()
        sys.exit(1)
    handler()  # type: ignore[no-untyped-call]
    sys.exit(0)


async def main_async(args: dict[str, Any]) -> None:
    """Main entry point for the loader application."""
    # Get config_name and run_id from arguments
    data_registry_id = args["config_name"]
    run_id = args["run_id"]

    # Bind run_id and config_id to context for all subsequent logs
    with log_bind(run_id=run_id, data_registry_id=data_registry_id):
        # Log storage and kvstore configuration
        logger.info(
            "APP_CONFIG_SETTINGS_SELECTED",
            storage_type=args["storage"],
            kvstore_type=args["kvstore"],
            credentials_provider_type=args["credentials_provider"],
        )

        # Create loader configuration with CLI arguments
        with observe_around(logger, "CREATE_loader_APP_CONFIG"):
            app_config = await create_loader_app_config(
                data_registry_id=data_registry_id,
                credentials_provider_type=args["credentials_provider"],
                storage_type=args["storage"],
                kv_store_type=args["kvstore"],
                **cast("dict[str, Any]", args.get("factory_kwargs", {})),
            )

        try:
            plan = None
            with observe_around(logger, "INITIALIZE_loader"):
                logger.info("loader_INITIALIZING", data_registry_id=data_registry_id)

                # Use YAML configuration (configuration system)
                config_dir = args.get("config_dir")
                stage = args.get("stage", "raw")
                step = args.get("step")

                logger.info(
                    "USING_YAML_CONFIG",
                    data_registry_id=data_registry_id,
                    config_dir=config_dir,
                    stage=stage,
                    step=step,
                )

                # Load YAML loader configuration directly via DataPipelineConfig
                from oc_pipeline_bus.config import DataPipelineConfig
                from data_loader_core.core import DataRegistryLoaderConfig
                from data_loader_core.loader import run_loader

                pipeline_config = DataPipelineConfig(
                    local_config_dir=config_dir,
                )

                yaml_config = pipeline_config.load_config(
                    DataRegistryLoaderConfig,
                    data_registry_id=data_registry_id,
                    step=step,
                )

                # Implement loader logic
                await run_loader(
                    app_config=app_config,
                    loader_config=yaml_config,
                    data_registry_id=data_registry_id,
                    stage=stage,
                )

        except KeyError:
            logger.exception(
                "UNKNOWN_CONFIG_ERROR",
                data_registry_id=data_registry_id,
            )
            raise
        except Exception as e:
            logger.exception(
                "loader_RUN_ERROR",
                data_registry_id=data_registry_id,
                error=str(e),
            )
            raise


if __name__ == "__main__":
    main()

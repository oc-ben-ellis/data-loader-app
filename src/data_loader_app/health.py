"""Health check and status endpoints for the data loader application.

This module provides WSGI-based health check endpoints and application status
monitoring functionality.
"""

import json
import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol

import structlog
from openc_python_common.observability import log_bind, observe_around

logger = structlog.get_logger(__name__)


class StartResponse(Protocol):
    """WSGI start_response callable protocol."""

    def __call__(
        self,
        status: str,
        response_headers: list[tuple[str, str]],
        exc_info: tuple[type[BaseException], BaseException, Any] | None = None,
    ) -> Callable[[bytes], None]:
        """Start response callable."""
        ...


class HealthCheck:
    """Simple health check implementation with heartbeat functionality."""

    def __init__(self, app_name: str = "data-loader-app") -> None:
        """Initialize the health check.

        Args:
            app_name: Name of the application for health check responses.
        """
        self.app_name = app_name
        self.start_time = time.time()
        self.checks: dict[str, Callable[[], bool]] = {}

    def add_check(self, name: str, check_func: Callable[[], bool]) -> None:
        """Add a health check function.

        Args:
            name: Name of the health check.
            check_func: Function that returns True if healthy, False otherwise.
        """
        self.checks[name] = check_func

    def is_healthy(self) -> bool:
        """Check if all registered health checks pass.

        Returns:
            True if all checks pass, False otherwise.
        """
        for name, check_func in self.checks.items():
            try:
                if not check_func():
                    logger.warning("HEALTH_CHECK_FAILED", check_name=name)
                    return False
            except Exception as e:
                logger.exception("HEALTH_CHECK_ERROR", check_name=name, error=str(e))
                return False
        return True

    def get_status(self) -> dict[str, Any]:
        """Get detailed application status.

        Returns:
            Dictionary containing application status information.
        """
        uptime = time.time() - self.start_time
        healthy = self.is_healthy()

        status: dict[str, Any] = {
            "app_name": self.app_name,
            "status": "healthy" if healthy else "unhealthy",
            "uptime_seconds": uptime,
            "timestamp": time.time(),
            "checks": {},
        }

        # Run individual checks and collect results
        for name, check_func in self.checks.items():
            try:
                result = check_func()
                status["checks"][name] = {
                    "status": "pass" if result else "fail",
                    "error": None,
                }
            except Exception as e:
                logger.exception(
                    "HEALTH_CHECK_ERROR_IN_STATUS", check_name=name, error=str(e)
                )
                status["checks"][name] = {"status": "error", "error": str(e)}

        return status


class SimpleWSGIRouter:
    """Simple prefix-based WSGI router for health check endpoints."""

    def __init__(self, health_check: HealthCheck) -> None:
        """Initialize the router.

        Args:
            health_check: HealthCheck instance to use for endpoints.
        """
        self.health_check = health_check
        self.routes = {
            "/health": self._health_endpoint,
            "/health/": self._health_endpoint,
            "/status": self._status_endpoint,
            "/status/": self._status_endpoint,
            "/heartbeat": self._heartbeat_endpoint,
            "/heartbeat/": self._heartbeat_endpoint,
        }

    def __call__(
        self, environ: dict[str, Any], start_response: StartResponse
    ) -> Iterable[bytes]:
        """WSGI application entry point.

        Args:
            environ: WSGI environment dictionary.
            start_response: WSGI start_response callable.

        Returns:
            Response body as list of bytes.
        """
        path = environ.get("PATH_INFO", "")
        method = environ.get("REQUEST_METHOD", "GET")

        if method != "GET":
            start_response("405 Method Not Allowed", [("Content-Type", "text/plain")])
            return [b"Method Not Allowed"]

        handler = self.routes.get(path)
        if handler:
            return handler(environ, start_response)
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found"]

    def _health_endpoint(
        self, _environ: dict[str, Any], start_response: StartResponse
    ) -> Iterable[bytes]:
        """Health check endpoint - returns simple healthy/unhealthy status.

        Args:
            environ: WSGI environment dictionary.
            start_response: WSGI start_response callable.

        Returns:
            Response body as list of bytes.
        """
        with log_bind(endpoint="health"), observe_around(logger, "HEALTH_CHECK"):
            healthy = self.health_check.is_healthy()
            status_code = "200 OK" if healthy else "503 Service Unavailable"
            status_text = "healthy" if healthy else "unhealthy"

            start_response(status_code, [("Content-Type", "application/json")])
            response = {"status": status_text}
            return [json.dumps(response).encode("utf-8")]

    def _status_endpoint(
        self, _environ: dict[str, Any], start_response: StartResponse
    ) -> Iterable[bytes]:
        """Status endpoint - returns detailed application status.

        Args:
            environ: WSGI environment dictionary.
            start_response: WSGI start_response callable.

        Returns:
            Response body as list of bytes.
        """
        with log_bind(endpoint="status"), observe_around(logger, "STATUS_CHECK"):
            status = self.health_check.get_status()
            status_code = (
                "200 OK" if status["status"] == "healthy" else "503 Service Unavailable"
            )

            start_response(status_code, [("Content-Type", "application/json")])
            return [json.dumps(status, indent=2).encode("utf-8")]

    def _heartbeat_endpoint(
        self, _environ: dict[str, Any], start_response: StartResponse
    ) -> list[bytes]:
        """Heartbeat endpoint - lightweight health check for load balancers.

        Args:
            environ: WSGI environment dictionary.
            start_response: WSGI start_response callable.

        Returns:
            Response body as list of bytes.
        """
        with log_bind(endpoint="heartbeat"), observe_around(logger, "HEARTBEAT_CHECK"):
            healthy = self.health_check.is_healthy()
            status_code = "200 OK" if healthy else "503 Service Unavailable"

            start_response(status_code, [("Content-Type", "text/plain")])
            return [b"OK" if healthy else b"FAIL"]


def create_health_app(app_name: str = "data-loader-app") -> SimpleWSGIRouter:
    """Create a WSGI application with health check endpoints.

    Args:
        app_name: Name of the application.

    Returns:
        WSGI application instance with health check endpoints.
    """
    health_check = HealthCheck(app_name)

    # Add basic health checks
    def always_healthy() -> bool:
        """Basic health check that always returns True."""
        return True

    health_check.add_check("basic", always_healthy)

    return SimpleWSGIRouter(health_check)

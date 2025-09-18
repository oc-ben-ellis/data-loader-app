"""Unit tests for health check functionality.

This module contains comprehensive unit tests for the health check
and WSGI router functionality.
"""

import json
from unittest.mock import MagicMock, patch

from data_loader_app.health import (
    HealthCheck,
    SimpleWSGIRouter,
    create_health_app,
)


class TestHealthCheck:
    """Test the HealthCheck class."""

    def test_health_check_initialization(self) -> None:
        """Test health check initialization."""
        health_check = HealthCheck("test-app")

        assert health_check.app_name == "test-app"
        assert health_check.start_time > 0
        assert len(health_check.checks) == 0

    def test_health_check_add_check(self) -> None:
        """Test adding health checks."""
        health_check = HealthCheck("test-app")

        def test_check() -> bool:
            return True

        health_check.add_check("test", test_check)

        assert "test" in health_check.checks
        assert health_check.checks["test"] == test_check

    def test_health_check_is_healthy_success(self) -> None:
        """Test health check when all checks pass."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        health_check.add_check("test1", passing_check)
        health_check.add_check("test2", passing_check)

        assert health_check.is_healthy() is True

    def test_health_check_is_healthy_failure(self) -> None:
        """Test health check when a check fails."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        def failing_check() -> bool:
            return False

        health_check.add_check("test1", passing_check)
        health_check.add_check("test2", failing_check)

        assert health_check.is_healthy() is False

    def test_health_check_is_healthy_exception(self) -> None:
        """Test health check when a check raises an exception."""
        health_check = HealthCheck("test-app")

        def exception_check() -> bool:
            raise Exception("Test error")

        health_check.add_check("test", exception_check)

        assert health_check.is_healthy() is False

    def test_health_check_get_status(self) -> None:
        """Test getting detailed status."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        def failing_check() -> bool:
            return False

        health_check.add_check("passing", passing_check)
        health_check.add_check("failing", failing_check)

        status = health_check.get_status()

        assert status["app_name"] == "test-app"
        assert status["status"] == "unhealthy"  # One check fails
        assert "uptime_seconds" in status
        assert "timestamp" in status
        assert "checks" in status

        # Check individual check results
        assert status["checks"]["passing"]["status"] == "pass"
        assert status["checks"]["passing"]["error"] is None
        assert status["checks"]["failing"]["status"] == "fail"
        assert status["checks"]["failing"]["error"] is None

    def test_health_check_get_status_with_exception(self) -> None:
        """Test getting status when a check raises an exception."""
        health_check = HealthCheck("test-app")

        def exception_check() -> bool:
            raise Exception("Test error")

        health_check.add_check("exception", exception_check)

        status = health_check.get_status()

        assert status["status"] == "unhealthy"
        assert status["checks"]["exception"]["status"] == "error"
        assert status["checks"]["exception"]["error"] == "Test error"


class TestSimpleWSGIRouter:
    """Test the SimpleWSGIRouter class."""

    def test_router_initialization(self) -> None:
        """Test router initialization."""
        health_check = HealthCheck("test-app")
        router = SimpleWSGIRouter(health_check)

        assert router.health_check == health_check
        assert "/health" in router.routes
        assert "/status" in router.routes
        assert "/heartbeat" in router.routes

    def test_router_health_endpoint_success(self) -> None:
        """Test health endpoint with successful health check."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        health_check.add_check("test", passing_check)

        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment
        environ = {"PATH_INFO": "/health", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        with (
            patch("data_loader_app.health.log_bind") as mock_log_bind,
            patch("data_loader_app.health.observe_around") as mock_observe_around,
        ):
            # Mock context managers
            mock_log_bind.return_value.__enter__ = MagicMock()
            mock_log_bind.return_value.__exit__ = MagicMock()
            mock_observe_around.return_value.__enter__ = MagicMock()
            mock_observe_around.return_value.__exit__ = MagicMock()

            response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        response_data = json.loads(response_list[0].decode("utf-8"))
        assert response_data["status"] == "healthy"

        # Verify start_response was called with 200 OK
        start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "application/json")]
        )

    def test_router_health_endpoint_failure(self) -> None:
        """Test health endpoint with failed health check."""
        health_check = HealthCheck("test-app")

        def failing_check() -> bool:
            return False

        health_check.add_check("test", failing_check)

        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment
        environ = {"PATH_INFO": "/health", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        with (
            patch("data_loader_app.health.log_bind") as mock_log_bind,
            patch("data_loader_app.health.observe_around") as mock_observe_around,
        ):
            # Mock context managers
            mock_log_bind.return_value.__enter__ = MagicMock()
            mock_log_bind.return_value.__exit__ = MagicMock()
            mock_observe_around.return_value.__enter__ = MagicMock()
            mock_observe_around.return_value.__exit__ = MagicMock()

            response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        response_data = json.loads(response_list[0].decode("utf-8"))
        assert response_data["status"] == "unhealthy"

        # Verify start_response was called with 503 Service Unavailable
        start_response.assert_called_once_with(
            "503 Service Unavailable", [("Content-Type", "application/json")]
        )

    def test_router_status_endpoint(self) -> None:
        """Test status endpoint."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        health_check.add_check("test", passing_check)

        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment
        environ = {"PATH_INFO": "/status", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        with (
            patch("data_loader_app.health.log_bind") as mock_log_bind,
            patch("data_loader_app.health.observe_around") as mock_observe_around,
        ):
            # Mock context managers
            mock_log_bind.return_value.__enter__ = MagicMock()
            mock_log_bind.return_value.__exit__ = MagicMock()
            mock_observe_around.return_value.__enter__ = MagicMock()
            mock_observe_around.return_value.__exit__ = MagicMock()

            response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        response_data = json.loads(response_list[0].decode("utf-8"))
        assert response_data["app_name"] == "test-app"
        assert response_data["status"] == "healthy"
        assert "uptime_seconds" in response_data
        assert "timestamp" in response_data
        assert "checks" in response_data

    def test_router_heartbeat_endpoint_success(self) -> None:
        """Test heartbeat endpoint with successful health check."""
        health_check = HealthCheck("test-app")

        def passing_check() -> bool:
            return True

        health_check.add_check("test", passing_check)

        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment
        environ = {"PATH_INFO": "/heartbeat", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        with (
            patch("data_loader_app.health.log_bind") as mock_log_bind,
            patch("data_loader_app.health.observe_around") as mock_observe_around,
        ):
            # Mock context managers
            mock_log_bind.return_value.__enter__ = MagicMock()
            mock_log_bind.return_value.__exit__ = MagicMock()
            mock_observe_around.return_value.__enter__ = MagicMock()
            mock_observe_around.return_value.__exit__ = MagicMock()

            response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        assert response_list[0] == b"OK"

        # Verify start_response was called with 200 OK
        start_response.assert_called_once_with(
            "200 OK", [("Content-Type", "text/plain")]
        )

    def test_router_heartbeat_endpoint_failure(self) -> None:
        """Test heartbeat endpoint with failed health check."""
        health_check = HealthCheck("test-app")

        def failing_check() -> bool:
            return False

        health_check.add_check("test", failing_check)

        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment
        environ = {"PATH_INFO": "/heartbeat", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        with (
            patch("data_loader_app.health.log_bind") as mock_log_bind,
            patch("data_loader_app.health.observe_around") as mock_observe_around,
        ):
            # Mock context managers
            mock_log_bind.return_value.__enter__ = MagicMock()
            mock_log_bind.return_value.__exit__ = MagicMock()
            mock_observe_around.return_value.__enter__ = MagicMock()
            mock_observe_around.return_value.__exit__ = MagicMock()

            response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        assert response_list[0] == b"FAIL"

        # Verify start_response was called with 503 Service Unavailable
        start_response.assert_called_once_with(
            "503 Service Unavailable", [("Content-Type", "text/plain")]
        )

    def test_router_unsupported_method(self) -> None:
        """Test router with unsupported HTTP method."""
        health_check = HealthCheck("test-app")
        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment with POST method
        environ = {"PATH_INFO": "/health", "REQUEST_METHOD": "POST"}
        start_response = MagicMock()

        response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        assert response_list[0] == b"Method Not Allowed"

        # Verify start_response was called with 405 Method Not Allowed
        start_response.assert_called_once_with(
            "405 Method Not Allowed", [("Content-Type", "text/plain")]
        )

    def test_router_not_found(self) -> None:
        """Test router with unknown path."""
        health_check = HealthCheck("test-app")
        router = SimpleWSGIRouter(health_check)

        # Mock WSGI environment with unknown path
        environ = {"PATH_INFO": "/unknown", "REQUEST_METHOD": "GET"}
        start_response = MagicMock()

        response = router(environ, start_response)

        # Verify response
        response_list = list(response)
        assert len(response_list) == 1
        assert response_list[0] == b"Not Found"

        # Verify start_response was called with 404 Not Found
        start_response.assert_called_once_with(
            "404 Not Found", [("Content-Type", "text/plain")]
        )


class TestCreateHealthApp:
    """Test the create_health_app function."""

    def test_create_health_app_default(self) -> None:
        """Test creating health app with default name."""
        app = create_health_app()

        assert isinstance(app, SimpleWSGIRouter)
        assert app.health_check.app_name == "data-loader-app"
        assert "basic" in app.health_check.checks

    def test_create_health_app_custom_name(self) -> None:
        """Test creating health app with custom name."""
        app = create_health_app("custom-app")

        assert isinstance(app, SimpleWSGIRouter)
        assert app.health_check.app_name == "custom-app"
        assert "basic" in app.health_check.checks

    def test_create_health_app_basic_check(self) -> None:
        """Test that basic health check is added."""
        app = create_health_app()

        # The basic check should always return True
        assert app.health_check.checks["basic"]() is True

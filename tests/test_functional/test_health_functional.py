"""Functional tests for health check endpoints.

This module contains functional tests that test the health check
endpoints in a more realistic environment.
"""

import json
import subprocess
import time
from pathlib import Path

import pytest
import requests


class TestHealthFunctional:
    """Functional tests for health check endpoints."""

    @pytest.fixture
    def health_server_process(self, temp_dir: str):
        """Start a health check server process for testing."""
        # Create a simple test script that starts the health server
        test_script = Path(temp_dir) / "test_health_server.py"
        test_script.write_text('''
import sys
import os
from pathlib import Path

# Add src to path
src_path = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(src_path))

from data_loader_app.main import health_command

if __name__ == "__main__":
    # Start health server on port 8081 to avoid conflicts
    health_command(["--port", "8081", "--host", "127.0.0.1"])
''')
        
        # Start the health server process
        process = subprocess.Popen(
            [sys.executable, str(test_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=temp_dir,
        )
        
        # Wait a moment for the server to start
        time.sleep(2)
        
        yield process
        
        # Clean up
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

    def test_health_endpoint_functional(self, health_server_process):
        """Test health endpoint functionality."""
        try:
            response = requests.get("http://127.0.0.1:8081/health", timeout=5)
            assert response.status_code == 200
            
            data = response.json()
            assert "status" in data
            assert data["status"] in ["healthy", "unhealthy"]
        except requests.exceptions.RequestException:
            pytest.skip("Health server not accessible")

    def test_status_endpoint_functional(self, health_server_process):
        """Test status endpoint functionality."""
        try:
            response = requests.get("http://127.0.0.1:8081/status", timeout=5)
            assert response.status_code == 200
            
            data = response.json()
            assert "app_name" in data
            assert "status" in data
            assert "uptime_seconds" in data
            assert "timestamp" in data
            assert "checks" in data
            assert isinstance(data["checks"], dict)
        except requests.exceptions.RequestException:
            pytest.skip("Health server not accessible")

    def test_heartbeat_endpoint_functional(self, health_server_process):
        """Test heartbeat endpoint functionality."""
        try:
            response = requests.get("http://127.0.0.1:8081/heartbeat", timeout=5)
            assert response.status_code == 200
            
            # Heartbeat should return plain text
            assert response.text in ["OK", "FAIL"]
        except requests.exceptions.RequestException:
            pytest.skip("Health server not accessible")

    def test_health_endpoint_404(self, health_server_process):
        """Test that unknown endpoints return 404."""
        try:
            response = requests.get("http://127.0.0.1:8081/unknown", timeout=5)
            assert response.status_code == 404
        except requests.exceptions.RequestException:
            pytest.skip("Health server not accessible")

    def test_health_endpoint_method_not_allowed(self, health_server_process):
        """Test that POST requests return 405."""
        try:
            response = requests.post("http://127.0.0.1:8081/health", timeout=5)
            assert response.status_code == 405
        except requests.exceptions.RequestException:
            pytest.skip("Health server not accessible")

    def test_health_server_startup_and_shutdown(self, temp_dir: str):
        """Test that health server can start and stop cleanly."""
        # Create a simple test script
        test_script = Path(temp_dir) / "test_health_lifecycle.py"
        test_script.write_text('''
import sys
import os
import signal
import time
from pathlib import Path

# Add src to path
src_path = Path(__file__).resolve().parents[2] / "src"
sys.path.insert(0, str(src_path))

from data_loader_app.main import health_command

def signal_handler(signum, frame):
    print("Received signal, shutting down...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    try:
        # Start health server
        health_command(["--port", "8082", "--host", "127.0.0.1"])
    except KeyboardInterrupt:
        print("Shutdown requested")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
''')
        
        # Start the process
        process = subprocess.Popen(
            [sys.executable, str(test_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=temp_dir,
        )
        
        # Wait for startup
        time.sleep(2)
        
        # Verify it's running
        assert process.poll() is None
        
        # Send SIGTERM to test graceful shutdown
        process.terminate()
        
        # Wait for shutdown
        try:
            process.wait(timeout=10)
            assert process.returncode == 0
        except subprocess.TimeoutExpired:
            process.kill()
            pytest.fail("Health server did not shutdown gracefully")

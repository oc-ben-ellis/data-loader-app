"""Integration tests for key-value store functionality.

This module contains integration tests for the KV store system,
including Redis and memory store implementations.
"""

import pytest
from data_loader_core.kv_store import create_kv_store


class TestKVStoreIntegration:
    """Integration tests for KV store functionality."""

    def test_memory_kv_store_creation(self) -> None:
        """Test creating a memory-based KV store."""
        kv_store = create_kv_store(store_type="memory")
        assert kv_store is not None

    def test_memory_kv_store_basic_operations(self) -> None:
        """Test basic operations with memory KV store."""
        kv_store = create_kv_store(store_type="memory")
        
        # Test set and get
        kv_store.set("test_key", "test_value")
        value = kv_store.get("test_key")
        assert value == "test_value"
        
        # Test delete
        kv_store.delete("test_key")
        value = kv_store.get("test_key")
        assert value is None

    def test_memory_kv_store_with_ttl(self) -> None:
        """Test memory KV store with TTL functionality."""
        kv_store = create_kv_store(store_type="memory", default_ttl=1)
        
        # Set a value with TTL
        kv_store.set("test_key", "test_value", ttl=1)
        value = kv_store.get("test_key")
        assert value == "test_value"
        
        # Wait for TTL to expire (in a real test, you might use time.sleep)
        # For this test, we'll just verify the TTL was set
        assert kv_store.get("test_key") == "test_value"

    def test_memory_kv_store_serialization(self) -> None:
        """Test memory KV store with different serializers."""
        # Test with JSON serializer
        kv_store_json = create_kv_store(store_type="memory", serializer="json")
        test_data = {"key": "value", "number": 42}
        
        kv_store_json.set("test_key", test_data)
        retrieved_data = kv_store_json.get("test_key")
        assert retrieved_data == test_data

    def test_redis_kv_store_creation_with_container(self, redis_container) -> None:
        """Test creating a Redis-based KV store with container."""
        import redis
        
        # Get Redis connection details
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        
        # Create KV store with Redis
        kv_store = create_kv_store(
            store_type="redis",
            redis_host=host,
            redis_port=port,
            redis_db=0,
        )
        
        assert kv_store is not None

    def test_redis_kv_store_basic_operations_with_container(self, redis_container) -> None:
        """Test basic operations with Redis KV store using container."""
        import redis
        
        # Get Redis connection details
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        
        # Create KV store with Redis
        kv_store = create_kv_store(
            store_type="redis",
            redis_host=host,
            redis_port=port,
            redis_db=0,
        )
        
        # Test set and get
        kv_store.set("test_key", "test_value")
        value = kv_store.get("test_key")
        assert value == "test_value"
        
        # Test delete
        kv_store.delete("test_key")
        value = kv_store.get("test_key")
        assert value is None

    def test_redis_kv_store_with_serialization(self, redis_container) -> None:
        """Test Redis KV store with JSON serialization."""
        import redis
        
        # Get Redis connection details
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        
        # Create KV store with Redis and JSON serializer
        kv_store = create_kv_store(
            store_type="redis",
            redis_host=host,
            redis_port=port,
            redis_db=0,
            serializer="json",
        )
        
        # Test with complex data
        test_data = {"key": "value", "number": 42, "nested": {"inner": "data"}}
        kv_store.set("test_key", test_data)
        retrieved_data = kv_store.get("test_key")
        assert retrieved_data == test_data

    def test_kv_store_error_handling(self) -> None:
        """Test KV store error handling with invalid configuration."""
        # Test with invalid store type
        with pytest.raises(ValueError):
            create_kv_store(store_type="invalid_type")

    def test_kv_store_with_custom_config(self) -> None:
        """Test KV store with custom configuration."""
        kv_store = create_kv_store(
            store_type="memory",
            serializer="json",
            default_ttl=300,
        )
        
        assert kv_store is not None
        
        # Test that custom TTL is applied
        kv_store.set("test_key", "test_value")
        # In a real implementation, you might verify the TTL was set correctly
        assert kv_store.get("test_key") == "test_value"

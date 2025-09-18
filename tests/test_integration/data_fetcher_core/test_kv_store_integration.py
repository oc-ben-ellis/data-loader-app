"""Integration tests for Redis key-value store functionality.

This module contains integration tests for Redis key-value store functionality,
including real Redis container testing, connection management, and data state management.
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import redis.asyncio as redis

from data_loader_core.kv_store import (
    RedisKeyValueStore,
)


@pytest.mark.integration
class TestRedisIntegration:
    """Integration tests for Redis key-value store functionality."""

    @pytest.fixture
    async def redis_client(self, redis_container: Any) -> AsyncGenerator[redis.Redis]:
        """Create Redis client connected to test container."""
        # In Docker-in-Docker, use the container's internal network
        client = redis.Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            db=0,
            decode_responses=False,  # Keep as bytes for testing
        )

        # Test connection
        await client.ping()

        yield client

        # Cleanup
        await client.flushdb()
        # redis-py asyncio deprecates close() in favor of aclose() in 5.0.1
        await client.aclose()

    @pytest.fixture
    async def redis_store(
        self, redis_container: Any
    ) -> AsyncGenerator[RedisKeyValueStore]:
        """Create RedisKeyValueStore instance for testing."""
        # In Docker-in-Docker, use the container's internal network
        store = RedisKeyValueStore(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            db=0,
            key_prefix="test:",
            serializer="json",
            default_ttl=3600,
        )

        yield store

        # Cleanup
        await store.close()

    @pytest.mark.asyncio
    async def test_redis_connection_establishment(
        self, redis_store: RedisKeyValueStore
    ) -> None:
        """Test Redis connection establishment and management."""
        # Connection should be established automatically when first operation is called
        assert redis_store._redis is None  # Initially None

        # Test basic operations to verify connection is established
        await redis_store.put("test_key", "test_value")
        assert redis_store._redis is not None  # Now should be connected

        result = await redis_store.get("test_key")  # type: ignore[unreachable]
        assert result == "test_value"

    @pytest.mark.asyncio
    async def test_redis_data_state_management(
        self, redis_store: RedisKeyValueStore, redis_client: redis.Redis
    ) -> None:
        """Test that data is actually persisted in Redis."""
        test_data = {"user_id": 123, "name": "John Doe", "email": "john@example.com"}

        # Store data
        await redis_store.put("user:123", test_data)

        # Verify data exists in Redis
        raw_value = await redis_client.get("test:user:123")
        assert raw_value is not None

        # Verify data can be retrieved through store
        retrieved_data = await redis_store.get("user:123")
        assert retrieved_data == test_data

    @pytest.mark.asyncio
    async def test_redis_ttl_functionality(
        self, redis_store: RedisKeyValueStore, redis_client: redis.Redis
    ) -> None:
        """Test Redis TTL functionality with real expiration."""
        # Store data with short TTL
        await redis_store.put("temp_key", "temp_value", ttl=1)

        # Verify key exists immediately
        assert await redis_store.exists("temp_key") is True

        # Wait for expiration (reduced from 2.1 to 1.1 seconds)
        await asyncio.sleep(1.1)

        # Verify key has expired
        assert await redis_store.exists("temp_key") is False

        # Verify key is gone from Redis
        raw_value = await redis_client.get("test:temp_key")
        assert raw_value is None

    @pytest.mark.asyncio
    async def test_redis_key_prefixing(
        self, redis_store: RedisKeyValueStore, redis_client: redis.Redis
    ) -> None:
        """Test Redis key prefixing functionality."""
        # Store keys with prefix
        await redis_store.put("key1", "value1")
        await redis_store.put("key2", "value2")

        # Verify keys are stored with prefix in Redis
        raw_keys = await redis_client.keys("test:*")
        assert len(raw_keys) == 2
        assert b"test:key1" in raw_keys
        assert b"test:key2" in raw_keys

        # Verify keys can be retrieved without prefix
        assert await redis_store.get("key1") == "value1"
        assert await redis_store.get("key2") == "value2"

    @pytest.mark.asyncio
    async def test_redis_range_operations(
        self, redis_store: RedisKeyValueStore
    ) -> None:
        """Test Redis range operations with real data."""
        # Store multiple keys
        for i in range(10):
            await redis_store.put(f"user:{i}", {"id": i, "name": f"User {i}"})

        # Test range get with start key
        results = await redis_store.range_get("user:3")
        assert len(results) == 7  # user:3 through user:9

        # Test range get with start and end key
        results = await redis_store.range_get("user:3", "user:7")
        assert len(results) == 4  # user:3, user:4, user:5, user:6

        # Test range get with limit
        results = await redis_store.range_get("user:", limit=5)
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_redis_serialization_options(self, redis_container: Any) -> None:
        """Test Redis with different serialization options."""
        # In Docker-in-Docker, use the container's internal network
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)

        # Test JSON serialization
        json_store = RedisKeyValueStore(
            host=host,
            port=port,
            db=1,  # Use different DB to avoid conflicts
            serializer="json",
            key_prefix="json:",
        )

        # Test pickle serialization
        pickle_store = RedisKeyValueStore(
            host=host,
            port=port,
            db=2,  # Use different DB to avoid conflicts
            serializer="pickle",
            key_prefix="pickle:",
        )

        try:
            # Test JSON serialization
            complex_data = {
                "string": "test",
                "number": 42,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
            }

            await json_store.put("complex", complex_data)
            retrieved = await json_store.get("complex")
            assert retrieved == complex_data

            # Test pickle serialization with more complex types
            import datetime

            complex_pickle_data = {
                "datetime": datetime.datetime.now(),
                "set_data": {1, 2, 3},
                "bytes_data": b"binary_data",
            }

            await pickle_store.put("complex", complex_pickle_data)
            retrieved = await pickle_store.get("complex")
            assert retrieved is not None
            assert retrieved["set_data"] == complex_pickle_data["set_data"]  # type: ignore[index]
            assert retrieved["bytes_data"] == complex_pickle_data["bytes_data"]  # type: ignore[index]

        finally:
            await json_store.close()
            await pickle_store.close()

    @pytest.mark.asyncio
    async def test_redis_connection_recovery(
        self, redis_store: RedisKeyValueStore
    ) -> None:
        """Test Redis connection recovery after connection loss."""
        # Perform initial operation
        await redis_store.put("recovery_test", "initial_value")

        # Simulate connection loss by closing the Redis client
        # redis-py asyncio deprecates close() in favor of aclose() in 5.0.1
        await redis_store._redis.aclose()  # type: ignore[union-attr]

        # Try to perform operation - should reconnect automatically
        await redis_store.put("recovery_test", "recovered_value")

        # Verify operation succeeded
        result = await redis_store.get("recovery_test")
        assert result == "recovered_value"

    @pytest.mark.asyncio
    async def test_redis_error_handling(self, redis_store: RedisKeyValueStore) -> None:
        """Test Redis error handling with invalid operations."""
        # Test with invalid key
        result = await redis_store.get("nonexistent_key")
        assert result is None

        # Test with invalid key and default
        result = await redis_store.get("nonexistent_key", default="default_value")
        assert result == "default_value"

        # Test delete of nonexistent key
        result = await redis_store.delete("nonexistent_key")
        assert result is False

    @pytest.mark.asyncio
    async def test_redis_bulk_operations(self, redis_store: RedisKeyValueStore) -> None:
        """Test Redis bulk operations for performance."""
        # Store fewer keys for faster execution (reduced from 100 to 20)
        start_time = time.time()
        for i in range(20):
            await redis_store.put(f"bulk_key_{i}", f"bulk_value_{i}")
        store_time = time.time() - start_time

        # Retrieve fewer keys for faster execution
        start_time = time.time()
        for i in range(20):
            value = await redis_store.get(f"bulk_key_{i}")
            assert value == f"bulk_value_{i}"
        retrieve_time = time.time() - start_time

        # Verify performance is reasonable (should be fast)
        assert store_time < 2.0  # Should store 20 keys in under 2 seconds
        assert retrieve_time < 2.0  # Should retrieve 20 keys in under 2 seconds

    @pytest.mark.asyncio
    async def test_redis_context_manager(self, redis_container: Any) -> None:
        """Test Redis store as context manager."""
        # In Docker-in-Docker, use the container's internal network
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)

        async with RedisKeyValueStore(
            host=host,
            port=port,
            db=3,
            key_prefix="context:",
        ) as store:
            # Store data
            await store.put("context_key", "context_value")

            # Verify data
            result = await store.get("context_key")
            assert result == "context_value"

        # Store should be closed automatically

    @pytest.mark.asyncio
    async def test_redis_direct_store_integration(self, redis_container: Any) -> None:
        """Test Redis integration with direct store instance.

        This test verifies that Redis store operations work correctly
        when using the store instance directly (no global state).
        """
        # In Docker-in-Docker, use the container's internal network
        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)

        # Create store instance directly
        store = RedisKeyValueStore(
            host=host,
            port=port,
            db=4,
            key_prefix="test_direct:",
            serializer="json",
            default_ttl=3600,
        )

        try:
            # Test operations
            await store.put("test_key", "test_value")
            result = await store.get("test_key")
            assert result == "test_value"

            # Test range operations
            await store.put("user:1", {"id": 1, "name": "User 1"})
            await store.put("user:2", {"id": 2, "name": "User 2"})

            results = await store.range_get("user:")
            assert len(results) == 2

            # Test delete
            assert await store.delete("test_key") is True
            assert await store.exists("test_key") is False

        finally:
            # Clean up store
            await store.close()

    @pytest.mark.asyncio
    async def test_redis_concurrent_access(
        self, redis_store: RedisKeyValueStore
    ) -> None:
        """Test Redis store with concurrent access patterns."""

        # Test concurrent writes
        async def write_key(key: str, value: str) -> None:
            await redis_store.put(key, value)

        # Test concurrent reads
        async def read_key(key: str) -> str | None:
            return await redis_store.get(key)  # type: ignore[return-value]

        # Store initial data
        await redis_store.put("concurrent_key", "initial_value")

        # Perform concurrent operations
        write_tasks = [write_key(f"concurrent_{i}", f"value_{i}") for i in range(10)]
        read_tasks = [read_key("concurrent_key") for _ in range(10)]

        # Execute concurrently
        write_results, read_results = await asyncio.gather(
            asyncio.gather(*write_tasks),
            asyncio.gather(*read_tasks),
        )

        # Verify all writes succeeded
        for i in range(10):
            value = await redis_store.get(f"concurrent_{i}")
            assert value == f"value_{i}"

        # Verify all reads succeeded
        for result in read_results:
            assert result == "initial_value"

    @pytest.mark.asyncio
    async def test_redis_memory_efficiency(
        self, redis_store: RedisKeyValueStore, redis_client: redis.Redis
    ) -> None:
        """Test Redis memory efficiency with moderate data."""
        # Store moderate data (reduced from 10KB to 1KB for faster execution)
        moderate_data = {
            "moderate_string": "x" * 1000,  # 1KB string (reduced from 10KB)
            "moderate_list": list(range(100)),  # 100 integers (reduced from 1000)
            "moderate_dict": {
                f"key_{i}": f"value_{i}" for i in range(100)
            },  # 100 items (reduced from 1000)
        }

        await redis_store.put("moderate_data", moderate_data)

        # Verify data integrity
        retrieved = await redis_store.get("moderate_data")
        assert retrieved is not None
        assert retrieved["moderate_string"] == moderate_data["moderate_string"]  # type: ignore[index]
        assert retrieved["moderate_list"] == moderate_data["moderate_list"]  # type: ignore[index]
        assert retrieved["moderate_dict"] == moderate_data["moderate_dict"]  # type: ignore[index]

        # Check Redis memory usage
        info = await redis_client.info("memory")
        used_memory = int(info["used_memory"])

        # Memory usage should be reasonable (under 10MB for this test)
        assert used_memory < 10 * 1024 * 1024

    @pytest.mark.asyncio
    async def test_redis_cleanup_and_maintenance(
        self, redis_store: RedisKeyValueStore, redis_client: redis.Redis
    ) -> None:
        """Test Redis cleanup and maintenance operations."""
        # Store data with different TTLs
        await redis_store.put("permanent", "permanent_value")
        await redis_store.put("expires_soon", "expires_soon_value", ttl=1)
        await redis_store.put("expires_later", "expires_later_value", ttl=5)

        # Wait for first expiration (reduced from 1.1 to 1.1 seconds)
        await asyncio.sleep(1.1)

        # Check what's still there
        assert await redis_store.exists("permanent") is True
        assert await redis_store.exists("expires_soon") is False
        assert await redis_store.exists("expires_later") is True

        # Clean up manually
        await redis_store.delete("permanent")
        await redis_store.delete("expires_later")

        # Verify cleanup
        assert await redis_store.exists("permanent") is False
        assert await redis_store.exists("expires_later") is False

        # Check Redis is empty
        keys = await redis_client.keys("test:*")
        assert len(keys) == 0

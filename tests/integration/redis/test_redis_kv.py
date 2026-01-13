# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the Redis-based KV store providers."""

import json
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from pydantic import BaseModel

from hexkit.custom_types import JsonObject
from hexkit.providers.redis.provider.kvstore import (
    RedisBytesKeyValueStore,
    RedisDtoKeyValueStore,
    RedisJsonKeyValueStore,
    RedisStrKeyValueStore,
)
from hexkit.providers.redis.testutils import (
    RedisFixture,
    redis_container_fixture,  # noqa: F401
    redis_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


class SimpleDto(BaseModel):
    """Simple test DTO."""

    name: str
    value: int


class ComplexDto(BaseModel):
    """Complex test DTO with nested structures."""

    id: str
    metadata: dict[str, str]
    items: list[int]
    is_active: bool = True


@pytest_asyncio.fixture()
async def json_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisJsonKeyValueStore, None]:
    """Provide a JSON kvstore bound to a test Redis instance."""
    async with RedisJsonKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisStrKeyValueStore, None]:
    """Provide a string kvstore bound to a test Redis instance."""
    async with RedisStrKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisBytesKeyValueStore, None]:
    """Provide a bytes kvstore bound to a test Redis instance."""
    async with RedisBytesKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO kvstore bound to a test Redis instance."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config, dto_model=SimpleDto
    ) as store:
        yield store


# Common tests for all store types (using separate test functions)


async def test_json_set_and_get(json_kvstore: RedisJsonKeyValueStore):
    """Test setting and getting a JSON value."""
    test_value: JsonObject = {"key": "value", "number": 42}
    await json_kvstore.set("test_key", test_value)
    assert await json_kvstore.get("test_key") == test_value


async def test_str_set_and_get(str_kvstore: RedisStrKeyValueStore):
    """Test setting and getting a string value."""
    test_value = "test string value"
    await str_kvstore.set("test_key", test_value)
    assert await str_kvstore.get("test_key") == test_value


async def test_bytes_set_and_get(bytes_kvstore: RedisBytesKeyValueStore):
    """Test setting and getting a bytes value."""
    test_value = b"test bytes value"
    await bytes_kvstore.set("test_key", test_value)
    assert await bytes_kvstore.get("test_key") == test_value


async def test_dto_set_and_get(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test setting and getting a DTO value."""
    test_value = SimpleDto(name="test", value=42)
    await dto_kvstore.set("test_key", test_value)
    assert await dto_kvstore.get("test_key") == test_value


async def test_json_get_nonexistent_key(json_kvstore: RedisJsonKeyValueStore):
    """Test getting a nonexistent key returns None."""
    assert await json_kvstore.get("nonexistent") is None


async def test_str_get_nonexistent_key(str_kvstore: RedisStrKeyValueStore):
    """Test getting a nonexistent key returns None."""
    assert await str_kvstore.get("nonexistent") is None


async def test_bytes_get_nonexistent_key(bytes_kvstore: RedisBytesKeyValueStore):
    """Test getting a nonexistent key returns None."""
    assert await bytes_kvstore.get("nonexistent") is None


async def test_dto_get_nonexistent_key(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test getting a nonexistent key returns None."""
    assert await dto_kvstore.get("nonexistent") is None


async def test_json_get_with_default(json_kvstore: RedisJsonKeyValueStore):
    """Test getting with a default value for JSON store."""
    default_value: JsonObject = {"default": True}
    assert await json_kvstore.get("nonexistent", default=default_value) == default_value


async def test_str_get_with_default(str_kvstore: RedisStrKeyValueStore):
    """Test getting with a default value for string store."""
    default_value = "default string"
    assert await str_kvstore.get("nonexistent", default=default_value) == default_value


async def test_bytes_get_with_default(bytes_kvstore: RedisBytesKeyValueStore):
    """Test getting with a default value for bytes store."""
    default_value = b"default bytes"
    assert (
        await bytes_kvstore.get("nonexistent", default=default_value) == default_value
    )


async def test_dto_get_with_default(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test getting with a default value for DTO store."""
    default_value = SimpleDto(name="default", value=0)
    assert await dto_kvstore.get("nonexistent", default=default_value) == default_value


async def test_json_update_existing_key(json_kvstore: RedisJsonKeyValueStore):
    """Test updating an existing JSON key."""
    await json_kvstore.set("update_key", {"version": 1})
    await json_kvstore.set("update_key", {"version": 2})
    assert await json_kvstore.get("update_key") == {"version": 2}


async def test_str_update_existing_key(str_kvstore: RedisStrKeyValueStore):
    """Test updating an existing string key."""
    await str_kvstore.set("update_key", "version 1")
    await str_kvstore.set("update_key", "version 2")
    assert await str_kvstore.get("update_key") == "version 2"


async def test_bytes_update_existing_key(bytes_kvstore: RedisBytesKeyValueStore):
    """Test updating an existing bytes key."""
    await bytes_kvstore.set("update_key", b"version 1")
    await bytes_kvstore.set("update_key", b"version 2")
    assert await bytes_kvstore.get("update_key") == b"version 2"


async def test_dto_update_existing_key(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test updating an existing DTO key."""
    await dto_kvstore.set("update_key", SimpleDto(name="v1", value=1))
    await dto_kvstore.set("update_key", SimpleDto(name="v2", value=2))
    assert await dto_kvstore.get("update_key") == SimpleDto(name="v2", value=2)


async def test_json_delete_existing_key(json_kvstore: RedisJsonKeyValueStore):
    """Test deleting an existing JSON key."""
    await json_kvstore.set("delete_me", {"to_delete": True})
    await json_kvstore.delete("delete_me")
    assert await json_kvstore.get("delete_me") is None


async def test_str_delete_existing_key(str_kvstore: RedisStrKeyValueStore):
    """Test deleting an existing string key."""
    await str_kvstore.set("delete_me", "delete me")
    await str_kvstore.delete("delete_me")
    assert await str_kvstore.get("delete_me") is None


async def test_bytes_delete_existing_key(bytes_kvstore: RedisBytesKeyValueStore):
    """Test deleting an existing bytes key."""
    await bytes_kvstore.set("delete_me", b"delete me")
    await bytes_kvstore.delete("delete_me")
    assert await bytes_kvstore.get("delete_me") is None


async def test_dto_delete_existing_key(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test deleting an existing DTO key."""
    await dto_kvstore.set("delete_me", SimpleDto(name="delete", value=0))
    await dto_kvstore.delete("delete_me")
    assert await dto_kvstore.get("delete_me") is None


async def test_json_delete_nonexistent_key(json_kvstore: RedisJsonKeyValueStore):
    """Test deleting a nonexistent JSON key does not raise."""
    await json_kvstore.delete("nonexistent")


async def test_str_delete_nonexistent_key(str_kvstore: RedisStrKeyValueStore):
    """Test deleting a nonexistent string key does not raise."""
    await str_kvstore.delete("nonexistent")


async def test_bytes_delete_nonexistent_key(bytes_kvstore: RedisBytesKeyValueStore):
    """Test deleting a nonexistent bytes key does not raise."""
    await bytes_kvstore.delete("nonexistent")


async def test_dto_delete_nonexistent_key(
    dto_kvstore: RedisDtoKeyValueStore[SimpleDto],
):
    """Test deleting a nonexistent DTO key does not raise."""
    await dto_kvstore.delete("nonexistent")


async def test_json_exists(json_kvstore: RedisJsonKeyValueStore):
    """Test the exists method for JSON store."""
    assert not await json_kvstore.exists("exists_key")
    await json_kvstore.set("exists_key", {"exists": True})
    assert await json_kvstore.exists("exists_key")
    await json_kvstore.delete("exists_key")
    assert not await json_kvstore.exists("exists_key")


async def test_str_exists(str_kvstore: RedisStrKeyValueStore):
    """Test the exists method for string store."""
    assert not await str_kvstore.exists("exists_key")
    await str_kvstore.set("exists_key", "exists")
    assert await str_kvstore.exists("exists_key")
    await str_kvstore.delete("exists_key")
    assert not await str_kvstore.exists("exists_key")


async def test_bytes_exists(bytes_kvstore: RedisBytesKeyValueStore):
    """Test the exists method for bytes store."""
    assert not await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.set("exists_key", b"exists")
    assert await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.delete("exists_key")
    assert not await bytes_kvstore.exists("exists_key")


async def test_dto_exists(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test the exists method for DTO store."""
    assert not await dto_kvstore.exists("exists_key")
    await dto_kvstore.set("exists_key", SimpleDto(name="exists", value=1))
    assert await dto_kvstore.exists("exists_key")
    await dto_kvstore.delete("exists_key")
    assert not await dto_kvstore.exists("exists_key")


async def test_json_multiple_keys(json_kvstore: RedisJsonKeyValueStore):
    """Test handling multiple JSON keys."""
    values = {"key1": {"value": 1}, "key2": {"value": 2}, "key3": {"value": 3}}
    for key, value in values.items():
        await json_kvstore.set(key, value)
    for key in values:
        assert await json_kvstore.exists(key)
    for key, expected in values.items():
        assert await json_kvstore.get(key) == expected
    await json_kvstore.delete("key2")
    assert await json_kvstore.exists("key1")
    assert not await json_kvstore.exists("key2")
    assert await json_kvstore.exists("key3")


async def test_str_multiple_keys(str_kvstore: RedisStrKeyValueStore):
    """Test handling multiple string keys."""
    values = {"key1": "value1", "key2": "value2", "key3": "value3"}
    for key, value in values.items():
        await str_kvstore.set(key, value)
    for key in values:
        assert await str_kvstore.exists(key)
    for key, expected in values.items():
        assert await str_kvstore.get(key) == expected
    await str_kvstore.delete("key2")
    assert await str_kvstore.exists("key1")
    assert not await str_kvstore.exists("key2")
    assert await str_kvstore.exists("key3")


async def test_bytes_multiple_keys(bytes_kvstore: RedisBytesKeyValueStore):
    """Test handling multiple bytes keys."""
    values = {"key1": b"value1", "key2": b"value2", "key3": b"value3"}
    for key, value in values.items():
        await bytes_kvstore.set(key, value)
    for key in values:
        assert await bytes_kvstore.exists(key)
    for key, expected in values.items():
        assert await bytes_kvstore.get(key) == expected
    await bytes_kvstore.delete("key2")
    assert await bytes_kvstore.exists("key1")
    assert not await bytes_kvstore.exists("key2")
    assert await bytes_kvstore.exists("key3")


async def test_dto_multiple_keys(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test handling multiple DTO keys."""
    values = {
        "key1": SimpleDto(name="first", value=1),
        "key2": SimpleDto(name="second", value=2),
        "key3": SimpleDto(name="third", value=3),
    }
    for key, value in values.items():
        await dto_kvstore.set(key, value)
    for key in values:
        assert await dto_kvstore.exists(key)
    for key, expected in values.items():
        assert await dto_kvstore.get(key) == expected
    await dto_kvstore.delete("key2")
    assert await dto_kvstore.exists("key1")
    assert not await dto_kvstore.exists("key2")
    assert await dto_kvstore.exists("key3")


# Type-specific tests


async def test_json_complex_nested_objects(json_kvstore: RedisJsonKeyValueStore):
    """Test storing complex nested JSON objects."""
    complex_value: JsonObject = {
        "string": "test",
        "number": 42,
        "float": 3.14,
        "boolean": True,
        "null": None,
        "array": [1, 2, 3, "four"],
        "nested": {
            "deep": {"value": "deeply nested", "list": [{"item": 1}, {"item": 2}]}
        },
    }
    await json_kvstore.set("complex", complex_value)
    assert await json_kvstore.get("complex") == complex_value


async def test_json_empty_object(json_kvstore: RedisJsonKeyValueStore):
    """Test storing and retrieving an empty JSON object."""
    await json_kvstore.set("empty", {})
    assert await json_kvstore.get("empty") == {}
    assert await json_kvstore.exists("empty")


async def test_str_multiline_strings(str_kvstore: RedisStrKeyValueStore):
    """Test storing multiline strings."""
    multiline = """Line 1
Line 2
Line 3"""
    await str_kvstore.set("multiline", multiline)
    assert await str_kvstore.get("multiline") == multiline


async def test_str_unicode_strings(str_kvstore: RedisStrKeyValueStore):
    """Test storing strings with unicode characters."""
    unicode_text = "Hello \U0001f30d!"
    await str_kvstore.set("unicode", unicode_text)
    assert await str_kvstore.get("unicode") == unicode_text


async def test_str_empty_string(str_kvstore: RedisStrKeyValueStore):
    """Test storing an empty string."""
    await str_kvstore.set("empty", "")
    assert await str_kvstore.get("empty") == ""
    assert await str_kvstore.exists("empty")


async def test_bytes_large_binary_data(bytes_kvstore: RedisBytesKeyValueStore):
    """Test storing large binary data."""
    large_data = b"x" * 100_000
    await bytes_kvstore.set("large", large_data)
    assert await bytes_kvstore.get("large") == large_data


async def test_bytes_binary_data_with_nulls(bytes_kvstore: RedisBytesKeyValueStore):
    """Test storing binary data with null bytes."""
    binary_data = b"\x00\x01\x02\xff\xfe\xfd"
    await bytes_kvstore.set("binary", binary_data)
    assert await bytes_kvstore.get("binary") == binary_data


async def test_bytes_empty_bytes(bytes_kvstore: RedisBytesKeyValueStore):
    """Test storing empty bytes."""
    await bytes_kvstore.set("empty", b"")
    assert await bytes_kvstore.get("empty") == b""
    assert await bytes_kvstore.exists("empty")


async def test_dto_simple_dto(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test storing and retrieving a simple DTO."""
    dto = SimpleDto(name="test", value=42)
    await dto_kvstore.set("simple", dto)
    retrieved = await dto_kvstore.get("simple")
    assert retrieved == dto
    assert isinstance(retrieved, SimpleDto)


async def test_dto_complex_dto(redis: RedisFixture):
    """Test storing and retrieving a complex DTO."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config, dto_model=ComplexDto
    ) as store:
        dto = ComplexDto(
            id="test123",
            metadata={"author": "John", "version": "1.0"},
            items=[1, 2, 3, 4, 5],
            is_active=True,
        )
        await store.set("complex", dto)
        retrieved = await store.get("complex")
        assert retrieved == dto
        assert isinstance(retrieved, ComplexDto)


async def test_dto_with_defaults(redis: RedisFixture):
    """Test DTOs with default values and custom prefix."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config,
        dto_model=ComplexDto,
        key_prefix="defaults:",
    ) as store:
        dto = ComplexDto(
            id="test456",
            metadata={"key": "value"},
            items=[10, 20],
        )
        await store.set("dto_with_defaults", dto)
        retrieved = await store.get("dto_with_defaults")
        assert retrieved == dto
        assert retrieved.is_active is True


async def test_dto_update_dto(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test updating a DTO value."""
    await dto_kvstore.set("dto_key", SimpleDto(name="first", value=1))
    await dto_kvstore.set("dto_key", SimpleDto(name="second", value=2))
    assert await dto_kvstore.get("dto_key") == SimpleDto(name="second", value=2)


# Whitebox/internals tests


async def test_default_prefix_stores_keys_as_is(redis: RedisFixture):
    """Verify default (empty) prefix stores keys without modification."""
    async with RedisStrKeyValueStore.construct(config=redis.config) as store:
        await store.set("plainkey", "value")
        raw = redis.client.get("plainkey")
        assert raw == b"value"


async def test_custom_prefix_is_used(redis: RedisFixture):
    """Verify custom prefix is applied to stored keys."""
    async with RedisStrKeyValueStore.construct(
        config=redis.config, key_prefix="app:"
    ) as store:
        await store.set("key", "value")
        assert redis.client.get("key") is None
        assert redis.client.get("app:key") == b"value"


async def test_prefix_isolation(redis: RedisFixture):
    """Verify different prefixes isolate stored keys."""
    async with (
        RedisJsonKeyValueStore.construct(
            config=redis.config, key_prefix="p1:"
        ) as store1,
        RedisJsonKeyValueStore.construct(
            config=redis.config, key_prefix="p2:"
        ) as store2,
    ):
        await store1.set("shared", {"id": 1})
        await store2.set("shared", {"id": 2})
        assert await store1.get("shared") == {"id": 1}
        assert await store2.get("shared") == {"id": 2}
        assert redis.client.get("p1:shared") is not None
        assert redis.client.get("p2:shared") is not None


async def test_json_whitebox_storage(redis: RedisFixture):
    """Verify JSON is stored as expected in raw Redis."""
    async with RedisJsonKeyValueStore.construct(config=redis.config) as store:
        payload: JsonObject = {"foo": True, "bar": False}
        await store.set("whitebox", payload)
        raw: bytes | None = redis.client.get("whitebox")  # type: ignore[assignment]
        assert raw is not None
        assert json.loads(raw.decode("utf-8")) == payload
        assert await store.get("whitebox") == payload


async def test_dto_whitebox_storage(redis: RedisFixture):
    """Verify DTOs are serialized correctly in raw Redis."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config, dto_model=SimpleDto, key_prefix="dto:"
    ) as store:
        dto = SimpleDto(name="white", value=99)
        await store.set("box", dto)
        raw: bytes | None = redis.client.get("dto:box")  # type: ignore[assignment]
        assert raw is not None
        assert json.loads(raw.decode("utf-8")) == dto.model_dump()
        assert await store.get("box") == dto


async def test_repr_contains_prefix(redis: RedisFixture):
    """Ensure repr includes the key prefix information."""
    async with RedisStrKeyValueStore.construct(
        config=redis.config, key_prefix="pref:"
    ) as store:
        rep = repr(store)
        assert "RedisStrKeyValueStore" in rep
        assert "'key_prefix': 'pref:'" in rep


# Type validation tests


async def test_json_set_with_wrong_type(json_kvstore: RedisJsonKeyValueStore):
    """Test that setting a non-dict value to JSON store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", "not a dict")

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", [1, 2, 3])

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", None)


async def test_str_set_with_wrong_type(str_kvstore: RedisStrKeyValueStore):
    """Test that setting a non-string value to string store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", {"dict": "value"})

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", b"bytes")

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", None)


async def test_bytes_set_with_wrong_type(bytes_kvstore: RedisBytesKeyValueStore):
    """Test that setting a non-bytes value to bytes store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", "string")

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", {"dict": "value"})

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", None)


async def test_dto_set_with_wrong_type(dto_kvstore: RedisDtoKeyValueStore[SimpleDto]):
    """Test that setting a non-DTO value to DTO store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", "not a dto")

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", {"name": "test", "value": 42})

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", None)


async def test_dto_set_with_different_dto_type(redis: RedisFixture):
    """Test that setting an instance of a different DTO type raises TypeError."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config, dto_model=SimpleDto
    ) as store:
        # ComplexDto is not SimpleDto
        complex_dto = ComplexDto(
            id="test",
            metadata={"key": "value"},
            items=[1, 2, 3],
        )
        with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
            await store.set("key", complex_dto)

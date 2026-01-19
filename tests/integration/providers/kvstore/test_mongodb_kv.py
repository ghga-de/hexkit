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

"""Test the MongoDB-based KV store providers."""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from pydantic import BaseModel

from hexkit.custom_types import JsonObject
from hexkit.providers.mongodb.provider.kvstore import (
    MongoDbBytesKeyValueStore,
    MongoDbDtoKeyValueStore,
    MongoDbJsonKeyValueStore,
    MongoDbStrKeyValueStore,
)
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


# Test DTO models
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


# Fixtures for each store type
@pytest_asyncio.fixture()
async def json_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbJsonKeyValueStore, None]:
    """Provide a JSON kvstore bound to a test collection."""
    async with MongoDbJsonKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbStrKeyValueStore, None]:
    """Provide a string kvstore bound to a test collection."""
    async with MongoDbStrKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbBytesKeyValueStore, None]:
    """Provide a bytes kvstore bound to a test collection."""
    async with MongoDbBytesKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO kvstore bound to a test collection."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config, dto_model=SimpleDto
    ) as store:
        yield store


# Common tests for all store types (using separate test functions)


async def test_json_set_and_get(json_kvstore: MongoDbJsonKeyValueStore):
    """Test setting and getting a JSON value."""
    test_value: JsonObject = {"key": "value", "number": 42}
    await json_kvstore.set("test_key", test_value)
    retrieved = await json_kvstore.get("test_key")
    assert retrieved == test_value


async def test_str_set_and_get(str_kvstore: MongoDbStrKeyValueStore):
    """Test setting and getting a string value."""
    test_value = "test string value"
    await str_kvstore.set("test_key", test_value)
    retrieved = await str_kvstore.get("test_key")
    assert retrieved == test_value


async def test_bytes_set_and_get(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test setting and getting a bytes value."""
    test_value = b"test bytes value"
    await bytes_kvstore.set("test_key", test_value)
    retrieved = await bytes_kvstore.get("test_key")
    assert retrieved == test_value


async def test_dto_set_and_get(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test setting and getting a DTO value."""
    test_value = SimpleDto(name="test", value=42)
    await dto_kvstore.set("test_key", test_value)
    retrieved = await dto_kvstore.get("test_key")
    assert retrieved == test_value


async def test_json_get_nonexistent_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test getting a nonexistent key."""
    result = await json_kvstore.get("nonexistent")
    assert result is None


async def test_str_get_nonexistent_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test getting a nonexistent key."""
    result = await str_kvstore.get("nonexistent")
    assert result is None


async def test_bytes_get_nonexistent_key(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test getting a nonexistent key."""
    result = await bytes_kvstore.get("nonexistent")
    assert result is None


async def test_dto_get_nonexistent_key(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test getting a nonexistent key."""
    result = await dto_kvstore.get("nonexistent")
    assert result is None


async def test_json_get_with_default(json_kvstore: MongoDbJsonKeyValueStore):
    """Test getting with a default value."""
    default_value: JsonObject = {"default": True}
    result = await json_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_str_get_with_default(str_kvstore: MongoDbStrKeyValueStore):
    """Test getting with a default value."""
    default_value = "default string"
    result = await str_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_bytes_get_with_default(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test getting with a default value."""
    default_value = b"default bytes"
    result = await bytes_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_dto_get_with_default(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test getting with a default value."""
    default_value = SimpleDto(name="default", value=0)
    result = await dto_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_json_update_existing_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test updating an existing key."""
    initial_value: JsonObject = {"version": 1}
    updated_value: JsonObject = {"version": 2}
    await json_kvstore.set("update_key", initial_value)
    await json_kvstore.set("update_key", updated_value)
    result = await json_kvstore.get("update_key")
    assert result == updated_value


async def test_str_update_existing_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test updating an existing key."""
    await str_kvstore.set("update_key", "version 1")
    await str_kvstore.set("update_key", "version 2")
    result = await str_kvstore.get("update_key")
    assert result == "version 2"


async def test_bytes_update_existing_key(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test updating an existing key."""
    await bytes_kvstore.set("update_key", b"version 1")
    await bytes_kvstore.set("update_key", b"version 2")
    result = await bytes_kvstore.get("update_key")
    assert result == b"version 2"


async def test_dto_update_existing_key(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test updating an existing key."""
    initial_value = SimpleDto(name="v1", value=1)
    updated_value = SimpleDto(name="v2", value=2)
    await dto_kvstore.set("update_key", initial_value)
    await dto_kvstore.set("update_key", updated_value)
    result = await dto_kvstore.get("update_key")
    assert result == updated_value


async def test_json_delete_existing_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test deleting an existing key."""
    test_value: JsonObject = {"to_delete": True}
    await json_kvstore.set("delete_me", test_value)
    await json_kvstore.delete("delete_me")
    result = await json_kvstore.get("delete_me")
    assert result is None


async def test_str_delete_existing_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test deleting an existing key."""
    await str_kvstore.set("delete_me", "delete me")
    await str_kvstore.delete("delete_me")
    result = await str_kvstore.get("delete_me")
    assert result is None


async def test_bytes_delete_existing_key(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test deleting an existing key."""
    await bytes_kvstore.set("delete_me", b"delete me")
    await bytes_kvstore.delete("delete_me")
    result = await bytes_kvstore.get("delete_me")
    assert result is None


async def test_dto_delete_existing_key(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test deleting an existing key."""
    test_value = SimpleDto(name="delete", value=0)
    await dto_kvstore.set("delete_me", test_value)
    await dto_kvstore.delete("delete_me")
    result = await dto_kvstore.get("delete_me")
    assert result is None


async def test_json_delete_nonexistent_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test deleting a nonexistent key (should not raise error)."""
    await json_kvstore.delete("nonexistent")


async def test_str_delete_nonexistent_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test deleting a nonexistent key (should not raise error)."""
    await str_kvstore.delete("nonexistent")


async def test_bytes_delete_nonexistent_key(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test deleting a nonexistent key (should not raise error)."""
    await bytes_kvstore.delete("nonexistent")


async def test_dto_delete_nonexistent_key(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test deleting a nonexistent key (should not raise error)."""
    await dto_kvstore.delete("nonexistent")


async def test_json_exists(json_kvstore: MongoDbJsonKeyValueStore):
    """Test the exists method."""
    test_value: JsonObject = {"exists": True}
    assert not await json_kvstore.exists("exists_key")
    await json_kvstore.set("exists_key", test_value)
    assert await json_kvstore.exists("exists_key")
    await json_kvstore.delete("exists_key")
    assert not await json_kvstore.exists("exists_key")


async def test_str_exists(str_kvstore: MongoDbStrKeyValueStore):
    """Test the exists method."""
    assert not await str_kvstore.exists("exists_key")
    await str_kvstore.set("exists_key", "exists")
    assert await str_kvstore.exists("exists_key")
    await str_kvstore.delete("exists_key")
    assert not await str_kvstore.exists("exists_key")


async def test_bytes_exists(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test the exists method."""
    assert not await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.set("exists_key", b"exists")
    assert await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.delete("exists_key")
    assert not await bytes_kvstore.exists("exists_key")


async def test_dto_exists(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test the exists method."""
    test_value = SimpleDto(name="exists", value=1)
    assert not await dto_kvstore.exists("exists_key")
    await dto_kvstore.set("exists_key", test_value)
    assert await dto_kvstore.exists("exists_key")
    await dto_kvstore.delete("exists_key")
    assert not await dto_kvstore.exists("exists_key")


async def test_json_multiple_keys(json_kvstore: MongoDbJsonKeyValueStore):
    """Test working with multiple keys simultaneously."""
    values = {"key1": {"value": 1}, "key2": {"value": 2}, "key3": {"value": 3}}

    for key, value in values.items():
        await json_kvstore.set(key, value)
    for key in values:
        assert await json_kvstore.exists(key)
    for key, expected_value in values.items():
        retrieved = await json_kvstore.get(key)
        assert retrieved == expected_value
    await json_kvstore.delete("key2")
    assert await json_kvstore.exists("key1")
    assert not await json_kvstore.exists("key2")
    assert await json_kvstore.exists("key3")


async def test_str_multiple_keys(str_kvstore: MongoDbStrKeyValueStore):
    """Test working with multiple keys simultaneously."""
    values = {"key1": "value1", "key2": "value2", "key3": "value3"}

    for key, value in values.items():
        await str_kvstore.set(key, value)
    for key in values:
        assert await str_kvstore.exists(key)
    for key, expected_value in values.items():
        retrieved = await str_kvstore.get(key)
        assert retrieved == expected_value
    await str_kvstore.delete("key2")
    assert await str_kvstore.exists("key1")
    assert not await str_kvstore.exists("key2")
    assert await str_kvstore.exists("key3")


async def test_bytes_multiple_keys(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test working with multiple keys simultaneously."""
    values = {"key1": b"value1", "key2": b"value2", "key3": b"value3"}

    for key, value in values.items():
        await bytes_kvstore.set(key, value)
    for key in values:
        assert await bytes_kvstore.exists(key)
    for key, expected_value in values.items():
        retrieved = await bytes_kvstore.get(key)
        assert retrieved == expected_value
    await bytes_kvstore.delete("key2")
    assert await bytes_kvstore.exists("key1")
    assert not await bytes_kvstore.exists("key2")
    assert await bytes_kvstore.exists("key3")


async def test_dto_multiple_keys(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test working with multiple keys simultaneously."""
    values = {
        "key1": SimpleDto(name="first", value=1),
        "key2": SimpleDto(name="second", value=2),
        "key3": SimpleDto(name="third", value=3),
    }

    for key, value in values.items():
        await dto_kvstore.set(key, value)
    for key in values:
        assert await dto_kvstore.exists(key)
    for key, expected_value in values.items():
        retrieved = await dto_kvstore.get(key)
        assert retrieved == expected_value
    await dto_kvstore.delete("key2")
    assert await dto_kvstore.exists("key1")
    assert not await dto_kvstore.exists("key2")
    assert await dto_kvstore.exists("key3")


# Type-specific tests


async def test_json_complex_nested_objects(json_kvstore: MongoDbJsonKeyValueStore):
    """Test storing and retrieving complex nested JSON objects."""
    complex_value: JsonObject = {
        "string": "test",
        "number": 42,
        "float": 3.14,
        "boolean": True,
        "null": None,
        "array": [1, 2, 3, "four"],
        "nested": {
            "deep": {
                "value": "deeply nested",
                "list": [{"item": 1}, {"item": 2}],
            }
        },
    }
    await json_kvstore.set("complex", complex_value)
    retrieved = await json_kvstore.get("complex")
    assert retrieved == complex_value


async def test_json_empty_object(json_kvstore: MongoDbJsonKeyValueStore):
    """Test storing and retrieving an empty JSON object."""
    empty_value: JsonObject = {}
    await json_kvstore.set("empty", empty_value)
    retrieved = await json_kvstore.get("empty")
    assert retrieved == empty_value
    assert await json_kvstore.exists("empty")


async def test_json_repr(json_kvstore: MongoDbJsonKeyValueStore):
    """Test the __repr__ method."""
    r = repr(json_kvstore)
    assert r.startswith("MongoDbJsonKeyValueStore({")
    assert "'collection_name': 'kvstore'" in r
    assert r.endswith("})")


async def test_str_multiline_strings(str_kvstore: MongoDbStrKeyValueStore):
    """Test storing multiline strings."""
    multiline = """Line 1
Line 2
Line 3"""
    await str_kvstore.set("multiline", multiline)
    retrieved = await str_kvstore.get("multiline")
    assert retrieved == multiline


async def test_str_unicode_strings(str_kvstore: MongoDbStrKeyValueStore):
    """Test storing strings with unicode characters."""
    unicode_text = "Hello \U0001f30d!"
    await str_kvstore.set("unicode", unicode_text)
    retrieved = await str_kvstore.get("unicode")
    assert retrieved == unicode_text


async def test_str_empty_string(str_kvstore: MongoDbStrKeyValueStore):
    """Test storing an empty string."""
    await str_kvstore.set("empty", "")
    retrieved = await str_kvstore.get("empty")
    assert retrieved == ""
    assert await str_kvstore.exists("empty")


async def test_str_repr(str_kvstore: MongoDbStrKeyValueStore):
    """Test the __repr__ method."""
    r = repr(str_kvstore)
    assert r.startswith("MongoDbStrKeyValueStore({")
    assert "'collection_name': 'kvstore'" in r
    assert r.endswith("})")


async def test_bytes_large_binary_data(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test storing large binary data."""
    large_data = b"x" * 100_000  # 100 kB of data
    await bytes_kvstore.set("large", large_data)
    retrieved = await bytes_kvstore.get("large")
    assert retrieved == large_data


async def test_bytes_binary_data_with_nulls(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test storing binary data with null bytes."""
    binary_data = b"\x00\x01\x02\xff\xfe\xfd"
    await bytes_kvstore.set("binary", binary_data)
    retrieved = await bytes_kvstore.get("binary")
    assert retrieved == binary_data


async def test_bytes_empty_bytes(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test storing empty bytes."""
    await bytes_kvstore.set("empty", b"")
    retrieved = await bytes_kvstore.get("empty")
    assert retrieved == b""
    assert await bytes_kvstore.exists("empty")


async def test_bytes_repr(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test the __repr__ method."""
    r = repr(bytes_kvstore)
    assert r.startswith("MongoDbBytesKeyValueStore({")
    assert "'collection_name': 'kvstore'" in r
    assert r.endswith("})")


async def test_dto_simple_dto(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test storing and retrieving a simple DTO."""
    dto = SimpleDto(name="test", value=42)
    await dto_kvstore.set("simple", dto)
    retrieved = await dto_kvstore.get("simple")
    assert retrieved == dto
    assert isinstance(retrieved, SimpleDto)


async def test_dto_complex_dto(mongodb: MongoDbFixture):
    """Test storing and retrieving a complex DTO."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config, dto_model=ComplexDto
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


async def test_dto_with_defaults(mongodb: MongoDbFixture):
    """Test DTOs with default values."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config,
        dto_model=ComplexDto,
        collection_name="defaults_dto_kvstore",
    ) as store:
        dto = ComplexDto(
            id="test456",
            metadata={"key": "value"},
            items=[10, 20],
            # is_active not specified, should use default True
        )
        await store.set("dto_with_defaults", dto)
        retrieved = await store.get("dto_with_defaults")
        assert retrieved == dto
        assert retrieved.is_active is True


async def test_dto_update_dto(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test updating a DTO."""
    dto1 = SimpleDto(name="first", value=1)
    await dto_kvstore.set("dto_key", dto1)

    dto2 = SimpleDto(name="second", value=2)
    await dto_kvstore.set("dto_key", dto2)

    retrieved = await dto_kvstore.get("dto_key")
    assert retrieved == dto2


async def test_dto_repr(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test the __repr__ method."""
    r = repr(dto_kvstore)
    assert r.startswith("MongoDbDtoKeyValueStore({")
    assert "'collection_name': 'kvstore'" in r
    assert r.endswith("})")


# Whitebox/internals tests


async def test_json_whitebox_default_collection(
    json_kvstore: MongoDbJsonKeyValueStore, mongodb: MongoDbFixture
):
    """Verify default collection and stored document via the DAO factory."""
    key = "some-key"
    value: JsonObject = {"foo": True, "bar": False}
    await json_kvstore.set(key, value)
    db = mongodb.dao_factory._db
    collections = await db.list_collection_names()
    assert "kvstore" in collections
    collection = db["kvstore"]
    doc = await collection.find_one({"_id": key})
    assert doc == {"_id": key, "value": value}


async def test_json_whitebox_non_default_collection(mongodb: MongoDbFixture):
    """Verify non-default collection and stored document."""
    key = "some-key"
    value: JsonObject = {"foo": True, "bar": False}
    collection_name = "customCollection"
    async with MongoDbJsonKeyValueStore.construct(
        config=mongodb.config, collection_name=collection_name
    ) as store:
        await store.set(key, value)
    db = mongodb.dao_factory._db
    collections = await db.list_collection_names()
    assert collection_name in collections
    assert "kvstore" not in collections
    collection = db[collection_name]
    doc = await collection.find_one({"_id": key})
    assert doc == {"_id": key, "value": value}


async def test_dto_whitebox_storage_format(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto], mongodb: MongoDbFixture
):
    """Verify how DTOs are stored in MongoDB."""
    dto = SimpleDto(name="test", value=42)
    await dto_kvstore.set("dto_key", dto)

    # Check raw storage format
    db = mongodb.dao_factory._db
    collection = db["kvstore"]
    doc = await collection.find_one({"_id": "dto_key"})
    assert doc is not None
    assert doc["_id"] == "dto_key"
    assert doc["value"] == {"name": "test", "value": 42}


# Collection isolation tests


async def test_collection_isolation(mongodb: MongoDbFixture):
    """Test that different collection names are isolated from each other."""
    async with (
        MongoDbJsonKeyValueStore.construct(
            config=mongodb.config, collection_name="collection1"
        ) as kvstore1,
        MongoDbJsonKeyValueStore.construct(
            config=mongodb.config, collection_name="collection2"
        ) as kvstore2,
        MongoDbStrKeyValueStore.construct(
            config=mongodb.config, collection_name="collection3"
        ) as kvstore3,
        MongoDbBytesKeyValueStore.construct(
            config=mongodb.config, collection_name="collection4"
        ) as kvstore4,
    ):
        # Set same key in different collections
        await kvstore1.set("key", {"store": 1})
        await kvstore2.set("key", {"store": 2})
        await kvstore3.set("key", "store3")
        await kvstore4.set("key", b"store4")

        # Verify each store has its own data
        value1 = await kvstore1.get("key")
        value2 = await kvstore2.get("key")
        value3 = await kvstore3.get("key")
        value4 = await kvstore4.get("key")

        assert value1 == {"store": 1}
        assert value2 == {"store": 2}
        assert value3 == "store3"
        assert value4 == b"store4"


# Type validation tests


async def test_json_set_with_wrong_type(json_kvstore: MongoDbJsonKeyValueStore):
    """Test that setting a non-dict value to JSON store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", "not a dict")

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", [1, 2, 3])

    with pytest.raises(TypeError, match="Value must be a dict"):
        await json_kvstore.set("key", None)


async def test_str_set_with_wrong_type(str_kvstore: MongoDbStrKeyValueStore):
    """Test that setting a non-string value to string store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", {"dict": "value"})

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", b"bytes")

    with pytest.raises(TypeError, match="Value must be of type str"):
        await str_kvstore.set("key", None)


async def test_bytes_set_with_wrong_type(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test that setting a non-bytes value to bytes store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", "string")

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", {"dict": "value"})

    with pytest.raises(TypeError, match="Value must be of type bytes"):
        await bytes_kvstore.set("key", None)


async def test_dto_set_with_wrong_type(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test that setting a non-DTO value to DTO store raises TypeError."""
    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", "not a dto")

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", {"name": "test", "value": 42})

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", 42)

    with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
        await dto_kvstore.set("key", None)


async def test_dto_set_with_different_dto_type(mongodb: MongoDbFixture):
    """Test that setting an instance of a different DTO type raises TypeError."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config, dto_model=SimpleDto
    ) as store:
        # ComplexDto is not SimpleDto
        complex_dto = ComplexDto(
            id="test",
            metadata={"key": "value"},
            items=[1, 2, 3],
        )
        with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
            await store.set("key", complex_dto)

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

"""Test the MongoDB-based KV store providers for Str, Bytes, and DTO."""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from pydantic import BaseModel

from hexkit.providers.mongodb.provider.kvstore import (
    MongoDbBytesKeyValueStore,
    MongoDbDtoKeyValueStore,
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


# Fixtures for String KV Store
@pytest_asyncio.fixture()
async def str_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbStrKeyValueStore, None]:
    """Provide a string kvstore bound to a test collection for this module."""
    async with MongoDbStrKeyValueStore.construct(
        config=mongodb.config, collection_name="str_kvstore"
    ) as store:
        yield store


# Fixtures for Bytes KV Store
@pytest_asyncio.fixture()
async def bytes_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbBytesKeyValueStore, None]:
    """Provide a bytes kvstore bound to a test collection for this module."""
    async with MongoDbBytesKeyValueStore.construct(
        config=mongodb.config, collection_name="bytes_kvstore"
    ) as store:
        yield store


# Fixtures for DTO KV Store
@pytest_asyncio.fixture()
async def dto_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO kvstore bound to a test collection for this module."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config,
        dto_model=SimpleDto,
        collection_name="dto_kvstore",
    ) as store:
        yield store


# Tests for MongoDbStrKeyValueStore
async def test_str_kvstore_set_and_get(str_kvstore: MongoDbStrKeyValueStore):
    """Test setting and getting a string value."""
    test_value = "Hello, World!"
    await str_kvstore.set("greeting", test_value)
    retrieved = await str_kvstore.get("greeting")
    assert retrieved == test_value


async def test_str_kvstore_get_nonexistent_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test getting a non-existent key returns None by default."""
    result = await str_kvstore.get("nonexistent")
    assert result is None


async def test_str_kvstore_get_with_default(str_kvstore: MongoDbStrKeyValueStore):
    """Test getting a non-existent key with a default value."""
    default_value = "default"
    result = await str_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_str_kvstore_update_existing_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test updating an existing key with a new value."""
    initial_value = "version 1"
    await str_kvstore.set("key1", initial_value)
    updated_value = "version 2"
    await str_kvstore.set("key1", updated_value)
    result = await str_kvstore.get("key1")
    assert result == updated_value


async def test_str_kvstore_delete_existing_key(str_kvstore: MongoDbStrKeyValueStore):
    """Test deleting an existing key."""
    test_value = "to delete"
    await str_kvstore.set("key1", test_value)
    assert await str_kvstore.exists("key1")
    await str_kvstore.delete("key1")
    result = await str_kvstore.get("key1")
    assert result is None
    assert not await str_kvstore.exists("key1")


async def test_str_kvstore_exists(str_kvstore: MongoDbStrKeyValueStore):
    """Test checking if keys exist."""
    assert not await str_kvstore.exists("some-key")
    await str_kvstore.set("some-key", "some value")
    assert await str_kvstore.exists("some-key")
    await str_kvstore.delete("some-key")
    assert not await str_kvstore.exists("some-key")


async def test_str_kvstore_multiline_string(str_kvstore: MongoDbStrKeyValueStore):
    """Test storing and retrieving multiline strings."""
    multiline = "Line 1\nLine 2\nLine 3\nTab:\tEnd"
    await str_kvstore.set("multiline", multiline)
    retrieved = await str_kvstore.get("multiline")
    assert retrieved == multiline


async def test_str_kvstore_unicode(str_kvstore: MongoDbStrKeyValueStore):
    """Test storing and retrieving unicode strings."""
    unicode_str = "Hello 世界 🌍 Привет"
    await str_kvstore.set("unicode", unicode_str)
    retrieved = await str_kvstore.get("unicode")
    assert retrieved == unicode_str


async def test_str_kvstore_repr(str_kvstore: MongoDbStrKeyValueStore):
    """Test the __repr__ method."""
    r = repr(str_kvstore)
    assert r.startswith("MongoDbStrKeyValueStore({")
    assert "'collection_name': 'str_kvstore'" in r


# Tests for MongoDbBytesKeyValueStore
async def test_bytes_kvstore_set_and_get(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test setting and getting binary data."""
    test_value = b"Binary data example"
    await bytes_kvstore.set("data", test_value)
    retrieved = await bytes_kvstore.get("data")
    assert retrieved == test_value


async def test_bytes_kvstore_get_nonexistent_key(
    bytes_kvstore: MongoDbBytesKeyValueStore,
):
    """Test getting a non-existent key returns None by default."""
    result = await bytes_kvstore.get("nonexistent")
    assert result is None


async def test_bytes_kvstore_get_with_default(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test getting a non-existent key with a default value."""
    default_value = b"default"
    result = await bytes_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_bytes_kvstore_update_existing_key(
    bytes_kvstore: MongoDbBytesKeyValueStore,
):
    """Test updating an existing key with new binary data."""
    initial_value = b"version 1"
    await bytes_kvstore.set("key1", initial_value)
    updated_value = b"version 2"
    await bytes_kvstore.set("key1", updated_value)
    result = await bytes_kvstore.get("key1")
    assert result == updated_value


async def test_bytes_kvstore_delete_existing_key(
    bytes_kvstore: MongoDbBytesKeyValueStore,
):
    """Test deleting an existing key."""
    test_value = b"to delete"
    await bytes_kvstore.set("key1", test_value)
    assert await bytes_kvstore.exists("key1")
    await bytes_kvstore.delete("key1")
    result = await bytes_kvstore.get("key1")
    assert result is None
    assert not await bytes_kvstore.exists("key1")


async def test_bytes_kvstore_exists(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test checking if keys exist."""
    assert not await bytes_kvstore.exists("some-key")
    await bytes_kvstore.set("some-key", b"some value")
    assert await bytes_kvstore.exists("some-key")
    await bytes_kvstore.delete("some-key")
    assert not await bytes_kvstore.exists("some-key")


async def test_bytes_kvstore_large_binary_data(
    bytes_kvstore: MongoDbBytesKeyValueStore,
):
    """Test storing and retrieving large binary data."""
    large_data = bytes(range(256)) * 100  # 25.6 KB
    await bytes_kvstore.set("large", large_data)
    retrieved = await bytes_kvstore.get("large")
    assert retrieved == large_data
    assert len(retrieved) == len(large_data)


async def test_bytes_kvstore_empty_bytes(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test storing and retrieving empty bytes."""
    empty_bytes = b""
    await bytes_kvstore.set("empty", empty_bytes)
    retrieved = await bytes_kvstore.get("empty")
    assert retrieved == empty_bytes


async def test_bytes_kvstore_repr(bytes_kvstore: MongoDbBytesKeyValueStore):
    """Test the __repr__ method."""
    r = repr(bytes_kvstore)
    assert r.startswith("MongoDbBytesKeyValueStore({")
    assert "'collection_name': 'bytes_kvstore'" in r


# Tests for MongoDbDtoKeyValueStore
async def test_dto_kvstore_set_and_get(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test setting and getting a DTO value."""
    test_dto = SimpleDto(name="test", value=42)
    await dto_kvstore.set("dto1", test_dto)
    retrieved = await dto_kvstore.get("dto1")
    assert retrieved is not None
    assert retrieved.name == test_dto.name
    assert retrieved.value == test_dto.value
    assert isinstance(retrieved, SimpleDto)


async def test_dto_kvstore_get_nonexistent_key(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test getting a non-existent key returns None by default."""
    result = await dto_kvstore.get("nonexistent")
    assert result is None


async def test_dto_kvstore_get_with_default(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test getting a non-existent key with a default value."""
    default_dto = SimpleDto(name="default", value=0)
    result = await dto_kvstore.get("nonexistent", default=default_dto)
    assert result == default_dto


async def test_dto_kvstore_update_existing_key(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test updating an existing key with a new DTO."""
    initial_dto = SimpleDto(name="initial", value=1)
    await dto_kvstore.set("key1", initial_dto)
    updated_dto = SimpleDto(name="updated", value=2)
    await dto_kvstore.set("key1", updated_dto)
    result = await dto_kvstore.get("key1")
    assert result is not None
    assert result.name == "updated"
    assert result.value == 2


async def test_dto_kvstore_delete_existing_key(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test deleting an existing key."""
    test_dto = SimpleDto(name="to_delete", value=99)
    await dto_kvstore.set("key1", test_dto)
    assert await dto_kvstore.exists("key1")
    await dto_kvstore.delete("key1")
    result = await dto_kvstore.get("key1")
    assert result is None
    assert not await dto_kvstore.exists("key1")


async def test_dto_kvstore_exists(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test checking if keys exist."""
    assert not await dto_kvstore.exists("some-key")
    test_dto = SimpleDto(name="test", value=123)
    await dto_kvstore.set("some-key", test_dto)
    assert await dto_kvstore.exists("some-key")
    await dto_kvstore.delete("some-key")
    assert not await dto_kvstore.exists("some-key")


async def test_dto_kvstore_complex_dto(mongodb: MongoDbFixture):
    """Test storing and retrieving complex DTOs."""
    complex_dto = ComplexDto(
        id="abc123",
        metadata={"author": "Alice", "version": "1.0"},
        items=[1, 2, 3, 4, 5],
        is_active=True,
    )

    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config,
        dto_model=ComplexDto,
        collection_name="complex_dto_store",
    ) as store:
        await store.set("complex", complex_dto)
        retrieved = await store.get("complex")
        assert retrieved is not None
        assert retrieved.id == complex_dto.id
        assert retrieved.metadata == complex_dto.metadata
        assert retrieved.items == complex_dto.items
        assert retrieved.is_active == complex_dto.is_active


async def test_dto_kvstore_multiple_dtos(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto],
):
    """Test working with multiple DTOs simultaneously."""
    dto1 = SimpleDto(name="first", value=1)
    dto2 = SimpleDto(name="second", value=2)
    dto3 = SimpleDto(name="third", value=3)

    await dto_kvstore.set("key1", dto1)
    await dto_kvstore.set("key2", dto2)
    await dto_kvstore.set("key3", dto3)

    retrieved1 = await dto_kvstore.get("key1")
    retrieved2 = await dto_kvstore.get("key2")
    retrieved3 = await dto_kvstore.get("key3")

    assert retrieved1 is not None and retrieved1.name == "first"
    assert retrieved2 is not None and retrieved2.name == "second"
    assert retrieved3 is not None and retrieved3.name == "third"


async def test_dto_kvstore_repr(dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto]):
    """Test the __repr__ method."""
    r = repr(dto_kvstore)
    assert r.startswith("MongoDbDtoKeyValueStore({")
    assert "'collection_name': 'dto_kvstore'" in r
    assert "SimpleDto" in r


async def test_dto_kvstore_whitebox_storage(
    dto_kvstore: MongoDbDtoKeyValueStore[SimpleDto], mongodb: MongoDbFixture
):
    """Verify DTOs are stored as dictionaries in MongoDB."""
    key = "test-dto"
    test_dto = SimpleDto(name="test", value=42)
    await dto_kvstore.set(key, test_dto)

    db = mongodb.dao_factory._db
    collection = db["dto_kvstore"]
    doc = await collection.find_one({"_id": key})
    assert doc is not None
    assert doc["_id"] == key
    assert doc["value"] == {"name": "test", "value": 42}


# Collection isolation tests
async def test_collection_isolation_str_bytes(mongodb: MongoDbFixture):
    """Test that string and bytes stores with same collection name are isolated."""
    collection_name = "shared_collection"
    async with (
        MongoDbStrKeyValueStore.construct(
            config=mongodb.config, collection_name=collection_name
        ) as str_store,
        MongoDbBytesKeyValueStore.construct(
            config=mongodb.config, collection_name=collection_name
        ) as bytes_store,
    ):
        # Both stores share the same collection, but values have different types
        await str_store.set("key1", "string value")
        await bytes_store.set("key2", b"bytes value")

        str_val = await str_store.get("key1")
        bytes_val = await bytes_store.get("key2")

        assert str_val == "string value"
        assert bytes_val == b"bytes value"

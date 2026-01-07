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

"""Test the MongoDB-based KV store provider."""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from hexkit.custom_types import JsonObject
from hexkit.providers.mongodb.provider.kvstore import MongoDbJsonKeyValueStore
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


@pytest_asyncio.fixture()
async def json_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbJsonKeyValueStore, None]:
    """Provide a kvstore bound to a test collection for this module."""
    async with MongoDbJsonKeyValueStore.construct(config=mongodb.config) as store:
        yield store


async def test_json_kvstore_set_and_get(json_kvstore: MongoDbJsonKeyValueStore):
    """Test setting and getting a value."""
    test_value: JsonObject = {"name": "test", "count": 42}
    await json_kvstore.set("answer", test_value)
    retrieved = await json_kvstore.get("answer")
    assert retrieved == test_value


async def test_json_kvstore_get_nonexistent_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test getting a non-existent key returns None by default."""
    result = await json_kvstore.get("nonexistent")
    assert result is None


async def test_json_kvstore_get_with_default(json_kvstore: MongoDbJsonKeyValueStore):
    """Test getting a non-existent key with a default value."""
    default_value: JsonObject = {"default": True}
    result = await json_kvstore.get("nonexistent", default=default_value)
    assert result == default_value


async def test_json_kvstore_update_existing_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test updating an existing key with a new value."""
    initial_value: JsonObject = {"version": 1}
    await json_kvstore.set("key1", initial_value)
    updated_value: JsonObject = {"version": 2}
    await json_kvstore.set("key1", updated_value)
    result = await json_kvstore.get("key1")
    assert result == updated_value


async def test_json_kvstore_delete_existing_key(json_kvstore: MongoDbJsonKeyValueStore):
    """Test deleting an existing key."""
    test_value: JsonObject = {"data": "to delete"}
    await json_kvstore.set("key1", test_value)
    assert await json_kvstore.exists("key1")
    await json_kvstore.delete("key1")
    result = await json_kvstore.get("key1")
    assert result is None
    assert not await json_kvstore.exists("key1")


async def test_json_kvstore_delete_nonexistent_key(
    json_kvstore: MongoDbJsonKeyValueStore,
):
    """Test deleting a non-existent key (should not raise an error)."""
    await json_kvstore.delete("nonexistent-key")


async def test_json_kvstore_exists(json_kvstore: MongoDbJsonKeyValueStore):
    """Test checking if keys exist."""
    assert not await json_kvstore.exists("some-key")
    await json_kvstore.set("some-key", {"exists": True})
    assert await json_kvstore.exists("some-key")
    await json_kvstore.delete("some-key")
    assert not await json_kvstore.exists("some-key")


async def test_json_kvstore_multiple_keys(json_kvstore: MongoDbJsonKeyValueStore):
    """Test working with multiple keys simultaneously."""
    values = {
        "key1": {"value": 1},
        "key2": {"value": 2},
        "key3": {"value": 3},
    }

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


async def test_json_kvstore_complex_json_objects(
    json_kvstore: MongoDbJsonKeyValueStore,
):
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


async def test_json_kvstore_empty_object(json_kvstore: MongoDbJsonKeyValueStore):
    """Test storing and retrieving an empty JSON object."""
    empty_value: JsonObject = {}
    await json_kvstore.set("empty", empty_value)

    retrieved = await json_kvstore.get("empty")
    assert retrieved == empty_value
    assert await json_kvstore.exists("empty")


async def test_json_kvstore_collection_isolation(mongodb: MongoDbFixture):
    """Test that different collection names are isolated."""
    async with (
        MongoDbJsonKeyValueStore.construct(
            config=mongodb.config, collection_name="collection1"
        ) as kvstore1,
        MongoDbJsonKeyValueStore.construct(
            config=mongodb.config, collection_name="collection2"
        ) as kvstore2,
    ):
        await kvstore1.set("key", {"store": 1})
        await kvstore2.set("key", {"store": 2})
        value1 = await kvstore1.get("key")
        value2 = await kvstore2.get("key")
        assert value1 == {"store": 1}
        assert value2 == {"store": 2}


async def test_json_kvstore_repr(json_kvstore: MongoDbJsonKeyValueStore):
    """Test the __repr__ method."""
    r = repr(json_kvstore)
    assert r.startswith("MongoDbJsonKeyValueStore({")
    assert "'collection_name': 'kvstore'" in r
    assert r.endswith("})")


async def test_json_kvstore_whitebox_default_collection(
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


async def test_json_kvstore_whitebox_non_default_collection(
    mongodb: MongoDbFixture,
):
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

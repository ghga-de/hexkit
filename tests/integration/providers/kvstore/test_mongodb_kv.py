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

"""Test the MongoDB-based KVStore providers."""

from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
import pytest_asyncio

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
from tests.integration.providers.kvstore.kvstore_test_suites import (
    BytesKVStoreTestSuite,
    DtoKVStoreTestSuite,
    JsonKVStoreTestSuite,
    SimpleDto,
    StrKVStoreTestSuite,
)

pytestmark = pytest.mark.asyncio()


# Fixtures for each store type
@pytest_asyncio.fixture()
async def json_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbJsonKeyValueStore, None]:
    """Provide a JSON KVStore bound to a test collection."""
    async with MongoDbJsonKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbStrKeyValueStore, None]:
    """Provide a string KVStore bound to a test collection."""
    async with MongoDbStrKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbBytesKeyValueStore, None]:
    """Provide a bytes KVStore bound to a test collection."""
    async with MongoDbBytesKeyValueStore.construct(config=mongodb.config) as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[MongoDbDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO KVStore bound to a test collection."""
    async with MongoDbDtoKeyValueStore.construct(
        config=mongodb.config, dto_model=SimpleDto
    ) as store:
        yield store


@pytest.fixture()
def dto_store_factory(
    mongodb: MongoDbFixture,
) -> Callable[..., AbstractAsyncContextManager[MongoDbDtoKeyValueStore[Any]]]:
    """Factory to construct DTO kvstores for arbitrary models."""

    def factory(dto_model, **kwargs):
        return MongoDbDtoKeyValueStore.construct(
            config=mongodb.config, dto_model=dto_model, **kwargs
        )

    return factory


@pytest.fixture()
def kvstore_custom_kwargs() -> dict[str, Any]:
    """Provider-specific kwargs for custom options tests."""
    return {"collection_name": "customCollectionName"}


# Common tests using shared test suites


class TestMongoDbJsonKVStore(JsonKVStoreTestSuite):
    """Tests for MongoDB JSON KVStore implementation."""


class TestMongoDbStrKVStore(StrKVStoreTestSuite):
    """Tests for MongoDB String KVStore implementation."""


class TestMongoDbBytesKVStore(BytesKVStoreTestSuite):
    """Tests for MongoDB Bytes KVStore implementation."""


class TestMongoDbDtoKVStore(DtoKVStoreTestSuite):
    """Tests for MongoDB DTO KVStore implementation."""


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

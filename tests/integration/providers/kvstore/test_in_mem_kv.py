# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Test the in-memory KVStore providers for testing."""

import re
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
import pytest_asyncio

from hexkit.providers.testing.kvstore import (
    InMemBytesKeyValueStore,
    InMemDtoKeyValueStore,
    InMemJsonKeyValueStore,
    InMemStrKeyValueStore,
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
async def json_kvstore() -> AsyncGenerator[InMemJsonKeyValueStore, None]:
    """Provide an in-memory JSON KVStore."""
    async with InMemJsonKeyValueStore.construct() as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore() -> AsyncGenerator[InMemStrKeyValueStore, None]:
    """Provide an in-memory string KVStore."""
    async with InMemStrKeyValueStore.construct() as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore() -> AsyncGenerator[InMemBytesKeyValueStore, None]:
    """Provide an in-memory bytes KVStore."""
    async with InMemBytesKeyValueStore.construct() as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore() -> AsyncGenerator[InMemDtoKeyValueStore[SimpleDto], None]:
    """Provide an in-memory DTO KVStore."""
    async with InMemDtoKeyValueStore.construct(dto_model=SimpleDto) as store:
        yield store


@pytest.fixture()
def dto_store_factory() -> Callable[..., AbstractAsyncContextManager[Any]]:
    """Factory to construct DTO KVStore for arbitrary models."""

    def factory(dto_model, **kwargs):
        return InMemDtoKeyValueStore.construct(dto_model=dto_model, **kwargs)

    return factory


@pytest.fixture()
def kvstore_custom_kwargs() -> dict[str, Any]:
    """Provider-specific kwargs for custom options tests."""
    return {"key_prefix": "custom:"}


# Common tests using shared test suites


class TestInMemJsonKVStore(JsonKVStoreTestSuite):
    """Tests for in-memory JSON KVStore implementation."""


class TestInMemStrKVStore(StrKVStoreTestSuite):
    """Tests for in-memory string KVStore implementation."""


class TestInMemBytesKVStore(BytesKVStoreTestSuite):
    """Tests for in-memory bytes KVStore implementation."""


class TestInMemDtoKVStore(DtoKVStoreTestSuite):
    """Tests for in-memory DTO KVStore implementation."""


# In-memory-specific tests


async def test_store_repr():
    """Test the repr of a string value store."""
    async with InMemStrKeyValueStore.construct(key_prefix="my-prefix:") as store:
        store_repr = repr(store)
        store_repr = re.sub(r"\s+", " ", store_repr).strip()
        assert store_repr == "InMemStrKeyValueStore(key_prefix='my-prefix:')"


async def test_prefix_isolation_with_shared_backing_store():
    """Verify different key prefixes isolate values in shared memory."""
    backing_store: dict[str, bytes] = {}

    async with (
        InMemJsonKeyValueStore.construct(
            key_prefix="p1:",
            backing_store=backing_store,
        ) as store1,
        InMemJsonKeyValueStore.construct(
            key_prefix="p2:",
            backing_store=backing_store,
        ) as store2,
    ):
        await store1.set("shared", {"id": 1})
        await store2.set("shared", {"id": 2})
        assert await store1.get("shared") == {"id": 1}
        assert await store2.get("shared") == {"id": 2}

    assert "p1:shared" in backing_store
    assert "p2:shared" in backing_store

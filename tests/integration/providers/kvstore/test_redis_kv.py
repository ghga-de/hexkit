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

"""Test the Redis-based KVStore providers."""

import json
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
import pytest_asyncio

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
    redis: RedisFixture,
) -> AsyncGenerator[RedisJsonKeyValueStore, None]:
    """Provide a JSON KVStore bound to a test Redis instance."""
    async with RedisJsonKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def str_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisStrKeyValueStore, None]:
    """Provide a string KVStore bound to a test Redis instance."""
    async with RedisStrKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def bytes_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisBytesKeyValueStore, None]:
    """Provide a bytes KVStore bound to a test Redis instance."""
    async with RedisBytesKeyValueStore.construct(config=redis.config) as store:
        yield store


@pytest_asyncio.fixture()
async def dto_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisDtoKeyValueStore[SimpleDto], None]:
    """Provide a DTO KVStore bound to a test Redis instance."""
    async with RedisDtoKeyValueStore.construct(
        config=redis.config, dto_model=SimpleDto
    ) as store:
        yield store


@pytest.fixture()
def dto_store_factory(
    redis: RedisFixture,
) -> Callable[..., AbstractAsyncContextManager[RedisDtoKeyValueStore[Any]]]:
    """Factory to construct DTO KVStore for arbitrary models."""

    def factory(dto_model, **kwargs):
        return RedisDtoKeyValueStore.construct(
            config=redis.config, dto_model=dto_model, **kwargs
        )

    return factory


@pytest.fixture()
def kvstore_custom_kwargs() -> dict[str, Any]:
    """Provider-specific kwargs for custom options tests."""
    return {"key_prefix": "custom:"}


# Common tests using shared test suites


class TestRedisJsonKVStore(JsonKVStoreTestSuite):
    """Tests for Redis JSON KVStore implementation."""


class TestRedisStrKVStore(StrKVStoreTestSuite):
    """Tests for Redis String KVStore implementation."""


class TestRedisBytesKVStore(BytesKVStoreTestSuite):
    """Tests for Redis Bytes KVStore implementation."""


class TestRedisDtoKVStore(DtoKVStoreTestSuite):
    """Tests for Redis DTO KVStore implementation."""


# Redis-specific tests


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
        payload = {"foo": True, "bar": False}
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

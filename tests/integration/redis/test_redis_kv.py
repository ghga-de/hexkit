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

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from hexkit.providers.redis.provider.kvstore import RedisBytesKeyValueStore
from hexkit.providers.redis.testutils import (
    RedisFixture,
    redis_container_fixture,  # noqa: F401
    redis_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


@pytest_asyncio.fixture()
async def bytes_kvstore(
    redis: RedisFixture,
) -> AsyncGenerator[RedisBytesKeyValueStore, None]:
    """Provide a bytes kvstore bound to a test Redis instance."""
    async with RedisBytesKeyValueStore.construct(config=redis.config) as store:
        yield store


async def test_bytes_set_and_get(bytes_kvstore: RedisBytesKeyValueStore):
    """Test setting and getting a bytes value."""
    test_value = b"test bytes value"
    await bytes_kvstore.set("test_key", test_value)
    retrieved = await bytes_kvstore.get("test_key")
    assert retrieved == test_value


async def test_bytes_delete(bytes_kvstore: RedisBytesKeyValueStore):
    """Test deleting a key."""
    test_value = b"delete me"
    await bytes_kvstore.set("delete_key", test_value)
    await bytes_kvstore.delete("delete_key")
    result = await bytes_kvstore.get("delete_key")
    assert result is None


async def test_bytes_exists(bytes_kvstore: RedisBytesKeyValueStore):
    """Test checking if a key exists."""
    test_value = b"exists"
    assert not await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.set("exists_key", test_value)
    assert await bytes_kvstore.exists("exists_key")
    await bytes_kvstore.delete("exists_key")
    assert not await bytes_kvstore.exists("exists_key")


async def test_bytes_get_with_default(bytes_kvstore: RedisBytesKeyValueStore):
    """Test getting with a default value."""
    default_value = b"default bytes"
    result = await bytes_kvstore.get("nonexistent", default=default_value)
    assert result == default_value

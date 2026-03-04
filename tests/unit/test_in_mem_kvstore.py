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

"""Tests for in-memory KVStore testing providers."""

import pytest
from pydantic import BaseModel

from hexkit.providers.testing.kvstore import (
    InMemBytesKeyValueStore,
    InMemDtoKeyValueStore,
    InMemJsonKeyValueStore,
    InMemStrKeyValueStore,
)

pytestmark = pytest.mark.asyncio()


class ItemDto(BaseModel):
    """Simple DTO for testing."""

    name: str
    value: int


async def test_str_get_set_delete_exists():
    """Test basic string KV operations."""
    store = InMemStrKeyValueStore()

    assert await store.get("missing") is None
    assert await store.get("missing", default="fallback") == "fallback"
    assert not await store.exists("name")

    await store.set("name", "Ada")
    assert await store.exists("name")
    assert await store.get("name") == "Ada"

    await store.delete("name")
    assert not await store.exists("name")
    assert await store.get("name") is None


async def test_json_roundtrip_returns_copy():
    """Ensure JSON values are round-tripped through serialization."""
    store = InMemJsonKeyValueStore()
    payload = {"nested": {"count": 7}}

    await store.set("obj", payload)
    retrieved = await store.get("obj")

    assert retrieved == payload
    assert retrieved is not payload

    assert retrieved is not None
    retrieved["nested"]["count"] = 9
    assert await store.get("obj") == {"nested": {"count": 7}}


async def test_dto_roundtrip_and_type_check():
    """Ensure DTO values are validated and deserialized as new instances."""
    store = InMemDtoKeyValueStore(dto_model=ItemDto)
    dto = ItemDto(name="test", value=1)

    await store.set("dto", dto)
    retrieved = await store.get("dto")

    assert retrieved == dto
    assert retrieved is not dto

    with pytest.raises(TypeError, match="Value must be of type ItemDto"):
        await store.set("dto", "bad")


async def test_key_prefix_isolation_with_shared_backing_store():
    """Verify key prefixes isolate values with a shared backing store."""
    backing_store: dict[str, bytes] = {}
    store1 = InMemBytesKeyValueStore(key_prefix="s1:", backing_store=backing_store)
    store2 = InMemBytesKeyValueStore(key_prefix="s2:", backing_store=backing_store)

    await store1.set("shared", b"one")
    await store2.set("shared", b"two")

    assert await store1.get("shared") == b"one"
    assert await store2.get("shared") == b"two"
    assert backing_store["s1:shared"] == b"one"
    assert backing_store["s2:shared"] == b"two"


async def test_construct_context_manager():
    """Test that the construct helper yields a functioning store."""
    async with InMemStrKeyValueStore.construct() as store:
        await store.set("key", "value")
        assert await store.get("key") == "value"

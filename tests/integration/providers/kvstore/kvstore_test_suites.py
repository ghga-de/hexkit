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

"""Shared test suite for KVStore protocol implementations.

This module provides reusable test suites verifying that an implementation
of the KVStore protocol family (JSON, String, Bytes, and DTO) works as expected.

Provider-specific test modules should inherit and provide appropriate fixtures.
"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
import pytest_asyncio
from pydantic import BaseModel

from hexkit.custom_types import JsonObject
from hexkit.protocols.kvstore import KeyValueStoreProtocol

pytestmark = pytest.mark.asyncio()


# Test DTO models shared across all providers
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


# Base test suite with common tests for all KVStore types
class BaseKVStoreTestSuite:
    """Base test suite with common tests for all KVStore implementations."""

    async def test_get_nonexistent_key(self, store):
        """Test getting a nonexistent key returns None."""
        assert await store.get("nonexistent") is None

    async def test_delete_nonexistent_key(self, store):
        """Test deleting a nonexistent key does not raise error."""
        await store.delete("nonexistent")

    async def test_exists(self, store, test_value):
        """Test the exists method."""
        assert not await store.exists("exists_key")
        await store.set("exists_key", test_value)
        assert await store.exists("exists_key")
        await store.delete("exists_key")
        assert not await store.exists("exists_key")

    async def test_repr(self, store):
        """Ensure the default repr shows the provider class name."""
        r = repr(store)
        assert r.startswith(f"{store.__class__.__name__}(")
        assert r.endswith(")")


# JSON KVStore test suite
class JsonKVStoreTestSuite(BaseKVStoreTestSuite):
    """Shared test suite for JSON KVStore implementations."""

    @pytest_asyncio.fixture()
    async def store(self, json_kvstore: KeyValueStoreProtocol[JsonObject]):
        """Provide the JSON KVStore instance for base tests."""
        return json_kvstore

    @pytest.fixture()
    def test_value(self) -> JsonObject:
        """Value used by base existence tests."""
        return {"exists": True}

    async def test_json_set_and_get(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test setting and getting a JSON value."""
        test_value: JsonObject = {"key": "value", "number": 42}
        await json_kvstore.set("test_key", test_value)
        assert await json_kvstore.get("test_key") == test_value

    async def test_json_get_nonexistent_key(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test getting a nonexistent key."""
        assert await json_kvstore.get("nonexistent") is None

    async def test_json_get_with_default(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test getting with a default value."""
        default_value: JsonObject = {"default": True}
        assert (
            await json_kvstore.get("nonexistent", default=default_value)
            == default_value
        )

    async def test_json_update_existing_key(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test updating an existing key."""
        await json_kvstore.set("update_key", {"version": 1})
        await json_kvstore.set("update_key", {"version": 2})
        assert await json_kvstore.get("update_key") == {"version": 2}

    async def test_json_delete_existing_key(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test deleting an existing key."""
        await json_kvstore.set("delete_me", {"to_delete": True})
        await json_kvstore.delete("delete_me")
        assert await json_kvstore.get("delete_me") is None

    async def test_json_delete_nonexistent_key(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test deleting a nonexistent key does not raise error."""
        await json_kvstore.delete("nonexistent")

    async def test_json_exists(self, json_kvstore: KeyValueStoreProtocol[JsonObject]):
        """Test the exists method."""
        assert not await json_kvstore.exists("exists_key")
        await json_kvstore.set("exists_key", {"exists": True})
        assert await json_kvstore.exists("exists_key")
        await json_kvstore.delete("exists_key")
        assert not await json_kvstore.exists("exists_key")

    async def test_json_multiple_keys(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test working with multiple keys simultaneously."""
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

    async def test_json_complex_nested_objects(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
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
        assert await json_kvstore.get("complex") == complex_value

    async def test_json_empty_object(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test storing and retrieving an empty JSON object."""
        await json_kvstore.set("empty", {})
        assert await json_kvstore.get("empty") == {}
        assert await json_kvstore.exists("empty")

    async def test_json_set_with_wrong_type(
        self, json_kvstore: KeyValueStoreProtocol[JsonObject]
    ):
        """Test that setting a non-dict value raises TypeError."""
        with pytest.raises(TypeError, match="Value must be a dict"):
            await json_kvstore.set("key", "not a dict")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be a dict"):
            await json_kvstore.set("key", 42)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be a dict"):
            await json_kvstore.set("key", [1, 2, 3])  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be a dict"):
            await json_kvstore.set("key", None)  # type: ignore[arg-type]


# String KVStore test suite
class StrKVStoreTestSuite(BaseKVStoreTestSuite):
    """Shared test suite for String KVStore implementations."""

    @pytest_asyncio.fixture()
    async def store(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Provide the string KVStore instance for base tests."""
        return str_kvstore

    @pytest.fixture()
    def test_value(self) -> str:
        """Value used by base existence tests."""
        return "exists"

    async def test_str_set_and_get(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test setting and getting a string value."""
        test_value = "test string value"
        await str_kvstore.set("test_key", test_value)
        assert await str_kvstore.get("test_key") == test_value

    async def test_str_get_nonexistent_key(
        self, str_kvstore: KeyValueStoreProtocol[str]
    ):
        """Test getting a nonexistent key."""
        assert await str_kvstore.get("nonexistent") is None

    async def test_str_get_with_default(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test getting with a default value."""
        default_value = "default string"
        assert (
            await str_kvstore.get("nonexistent", default=default_value) == default_value
        )

    async def test_str_update_existing_key(
        self, str_kvstore: KeyValueStoreProtocol[str]
    ):
        """Test updating an existing key."""
        await str_kvstore.set("update_key", "version 1")
        await str_kvstore.set("update_key", "version 2")
        assert await str_kvstore.get("update_key") == "version 2"

    async def test_str_delete_existing_key(
        self, str_kvstore: KeyValueStoreProtocol[str]
    ):
        """Test deleting an existing key."""
        await str_kvstore.set("delete_me", "delete me")
        await str_kvstore.delete("delete_me")
        assert await str_kvstore.get("delete_me") is None

    async def test_str_delete_nonexistent_key(
        self, str_kvstore: KeyValueStoreProtocol[str]
    ):
        """Test deleting a nonexistent key does not raise error."""
        await str_kvstore.delete("nonexistent")

    async def test_str_exists(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test the exists method."""
        assert not await str_kvstore.exists("exists_key")
        await str_kvstore.set("exists_key", "exists")
        assert await str_kvstore.exists("exists_key")
        await str_kvstore.delete("exists_key")
        assert not await str_kvstore.exists("exists_key")

    async def test_str_multiple_keys(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test working with multiple keys simultaneously."""
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

    async def test_str_multiline_strings(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test storing multiline strings."""
        multiline = """Line 1
Line 2
Line 3"""
        await str_kvstore.set("multiline", multiline)
        assert await str_kvstore.get("multiline") == multiline

    async def test_str_unicode_strings(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test storing strings with unicode characters."""
        unicode_text = "Hello \U0001f30d!"
        await str_kvstore.set("unicode", unicode_text)
        assert await str_kvstore.get("unicode") == unicode_text

    async def test_str_empty_string(self, str_kvstore: KeyValueStoreProtocol[str]):
        """Test storing an empty string."""
        await str_kvstore.set("empty", "")
        assert await str_kvstore.get("empty") == ""
        assert await str_kvstore.exists("empty")

    async def test_str_set_with_wrong_type(
        self, str_kvstore: KeyValueStoreProtocol[str]
    ):
        """Test that setting a non-string value raises TypeError."""
        with pytest.raises(TypeError, match="Value must be of type str"):
            await str_kvstore.set("key", 42)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type str"):
            await str_kvstore.set("key", {"dict": "value"})  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type str"):
            await str_kvstore.set("key", b"bytes")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type str"):
            await str_kvstore.set("key", None)  # type: ignore[arg-type]


# Bytes KVStore test suite
class BytesKVStoreTestSuite(BaseKVStoreTestSuite):
    """Shared test suite for Bytes KVStore implementations."""

    @pytest_asyncio.fixture()
    async def store(self, bytes_kvstore: KeyValueStoreProtocol[bytes]):
        """Provide the bytes KVStore instance for base tests."""
        return bytes_kvstore

    @pytest.fixture()
    def test_value(self) -> bytes:
        """Value used by base existence tests."""
        return b"exists"

    async def test_bytes_set_and_get(self, bytes_kvstore: KeyValueStoreProtocol[bytes]):
        """Test setting and getting a bytes value."""
        test_value = b"test bytes value"
        await bytes_kvstore.set("test_key", test_value)
        assert await bytes_kvstore.get("test_key") == test_value

    async def test_bytes_get_nonexistent_key(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test getting a nonexistent key."""
        assert await bytes_kvstore.get("nonexistent") is None

    async def test_bytes_get_with_default(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test getting with a default value."""
        default_value = b"default bytes"
        assert (
            await bytes_kvstore.get("nonexistent", default=default_value)
            == default_value
        )

    async def test_bytes_update_existing_key(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test updating an existing key."""
        await bytes_kvstore.set("update_key", b"version 1")
        await bytes_kvstore.set("update_key", b"version 2")
        assert await bytes_kvstore.get("update_key") == b"version 2"

    async def test_bytes_delete_existing_key(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test deleting an existing key."""
        await bytes_kvstore.set("delete_me", b"delete me")
        await bytes_kvstore.delete("delete_me")
        assert await bytes_kvstore.get("delete_me") is None

    async def test_bytes_delete_nonexistent_key(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test deleting a nonexistent key does not raise error."""
        await bytes_kvstore.delete("nonexistent")

    async def test_bytes_exists(self, bytes_kvstore: KeyValueStoreProtocol[bytes]):
        """Test the exists method."""
        assert not await bytes_kvstore.exists("exists_key")
        await bytes_kvstore.set("exists_key", b"exists")
        assert await bytes_kvstore.exists("exists_key")
        await bytes_kvstore.delete("exists_key")
        assert not await bytes_kvstore.exists("exists_key")

    async def test_bytes_multiple_keys(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test working with multiple keys simultaneously."""
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

    async def test_bytes_large_binary_data(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test storing large binary data."""
        large_data = b"x" * 100_000
        await bytes_kvstore.set("large", large_data)
        assert await bytes_kvstore.get("large") == large_data

    async def test_bytes_binary_data_with_nulls(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test storing binary data with null bytes."""
        binary_data = b"\x00\x01\x02\xff\xfe\xfd"
        await bytes_kvstore.set("binary", binary_data)
        assert await bytes_kvstore.get("binary") == binary_data

    async def test_bytes_empty_bytes(self, bytes_kvstore: KeyValueStoreProtocol[bytes]):
        """Test storing empty bytes."""
        await bytes_kvstore.set("empty", b"")
        assert await bytes_kvstore.get("empty") == b""
        assert await bytes_kvstore.exists("empty")

    async def test_bytes_set_with_wrong_type(
        self, bytes_kvstore: KeyValueStoreProtocol[bytes]
    ):
        """Test that setting a non-bytes value raises TypeError."""
        with pytest.raises(TypeError, match="Value must be of type bytes"):
            await bytes_kvstore.set("key", "string")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type bytes"):
            await bytes_kvstore.set("key", 42)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type bytes"):
            await bytes_kvstore.set("key", {"dict": "value"})  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type bytes"):
            await bytes_kvstore.set("key", None)  # type: ignore[arg-type]


# DTO KVStore test suite
class DtoKVStoreTestSuite(BaseKVStoreTestSuite):
    """Shared test suite for DTO KVStore implementations."""

    @pytest_asyncio.fixture()
    async def store(self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]):
        """Provide the DTO KVStore instance for base tests."""
        return dto_kvstore

    @pytest.fixture()
    def test_value(self) -> SimpleDto:
        """Value used by base existence tests."""
        return SimpleDto(name="exists", value=1)

    async def test_dto_set_and_get(self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]):
        """Test setting and getting a DTO value."""
        test_value = SimpleDto(name="test", value=42)
        await dto_kvstore.set("test_key", test_value)
        retrieved = await dto_kvstore.get("test_key")
        assert retrieved == test_value
        assert isinstance(retrieved, SimpleDto)

    async def test_dto_get_nonexistent_key(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test getting a nonexistent key."""
        assert await dto_kvstore.get("nonexistent") is None

    async def test_dto_get_with_default(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test getting with a default value."""
        default_value = SimpleDto(name="default", value=0)
        assert (
            await dto_kvstore.get("nonexistent", default=default_value) == default_value
        )

    async def test_dto_update_existing_key(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test updating an existing key."""
        await dto_kvstore.set("update_key", SimpleDto(name="v1", value=1))
        await dto_kvstore.set("update_key", SimpleDto(name="v2", value=2))
        assert await dto_kvstore.get("update_key") == SimpleDto(name="v2", value=2)

    async def test_dto_delete_existing_key(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test deleting an existing key."""
        await dto_kvstore.set("delete_me", SimpleDto(name="delete", value=0))
        await dto_kvstore.delete("delete_me")
        assert await dto_kvstore.get("delete_me") is None

    async def test_dto_delete_nonexistent_key(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test deleting a nonexistent key does not raise error."""
        await dto_kvstore.delete("nonexistent")

    async def test_dto_exists(self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]):
        """Test the exists method."""
        assert not await dto_kvstore.exists("exists_key")
        await dto_kvstore.set("exists_key", SimpleDto(name="exists", value=1))
        assert await dto_kvstore.exists("exists_key")
        await dto_kvstore.delete("exists_key")
        assert not await dto_kvstore.exists("exists_key")

    async def test_dto_multiple_keys(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
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
        for key, expected in values.items():
            assert await dto_kvstore.get(key) == expected
        await dto_kvstore.delete("key2")
        assert await dto_kvstore.exists("key1")
        assert not await dto_kvstore.exists("key2")
        assert await dto_kvstore.exists("key3")

    async def test_dto_simple_dto(self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]):
        """Test storing and retrieving a simple DTO."""
        dto = SimpleDto(name="test", value=42)
        await dto_kvstore.set("simple", dto)
        retrieved = await dto_kvstore.get("simple")
        assert retrieved == dto
        assert isinstance(retrieved, SimpleDto)

    async def test_dto_update_dto(self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]):
        """Test updating a DTO."""
        await dto_kvstore.set("dto_key", SimpleDto(name="first", value=1))
        await dto_kvstore.set("dto_key", SimpleDto(name="second", value=2))
        assert await dto_kvstore.get("dto_key") == SimpleDto(name="second", value=2)

    async def test_dto_set_with_wrong_type(
        self, dto_kvstore: KeyValueStoreProtocol[SimpleDto]
    ):
        """Test that setting a non-DTO value raises TypeError."""
        with pytest.raises(TypeError, match="Value must be of type"):
            await dto_kvstore.set("key", "not a dto")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type"):
            await dto_kvstore.set("key", {"name": "test", "value": 42})  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type"):
            await dto_kvstore.set("key", 42)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Value must be of type"):
            await dto_kvstore.set("key", None)  # type: ignore[arg-type]

    async def test_dto_set_with_different_dto_type(
        self,
        dto_store_factory: Callable[
            ..., AbstractAsyncContextManager[KeyValueStoreProtocol[SimpleDto]]
        ],
        kvstore_custom_kwargs: dict[str, Any],
    ):
        """Test that setting an instance of another DTO type raises TypeError."""
        async with dto_store_factory(SimpleDto, **kvstore_custom_kwargs) as store:
            complex_dto = ComplexDto(
                id="test",
                metadata={"key": "value"},
                items=[1, 2, 3],
            )
            with pytest.raises(TypeError, match="Value must be of type SimpleDto"):
                await store.set("key", complex_dto)  # type: ignore[arg-type]

    async def test_dto_repr_custom_kwargs(
        self,
        dto_store_factory: Callable[
            ..., AbstractAsyncContextManager[KeyValueStoreProtocol[SimpleDto]]
        ],
        kvstore_custom_kwargs: dict[str, Any],
    ):
        """Ensure repr includes class name and custom kwargs in key=value form."""
        async with dto_store_factory(SimpleDto, **kvstore_custom_kwargs) as store:
            r = repr(store)
            assert r.startswith(f"{store.__class__.__name__}(")
            assert "dto_model=SimpleDto" in r
            custom_kwargs_repr = " ".join(
                f"{key}={value!r}" for key, value in kvstore_custom_kwargs.items()
            )
            assert custom_kwargs_repr in r
            assert r.endswith(")")

    async def test_dto_complex_dto(
        self,
        dto_store_factory: Callable[
            ..., AbstractAsyncContextManager[KeyValueStoreProtocol[ComplexDto]]
        ],
    ):
        """Test storing and retrieving a complex DTO via provided factory."""
        async with dto_store_factory(ComplexDto) as store:
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

    async def test_dto_with_custom_options(
        self,
        dto_store_factory: Callable[
            ..., AbstractAsyncContextManager[KeyValueStoreProtocol[ComplexDto]]
        ],
        kvstore_custom_kwargs: dict[str, Any],
    ):
        """Test DTOs with provider-specific custom kwargs."""
        async with dto_store_factory(ComplexDto, **kvstore_custom_kwargs) as store:
            dto = ComplexDto(
                id="test456",
                metadata={"key": "value"},
                items=[10, 20],
                # is_active not specified, should use default True
            )
            await store.set("custom-store-object", dto)
            retrieved = await store.get("custom-store-object")
            assert retrieved == dto
            assert retrieved is not None
            assert retrieved.is_active is True

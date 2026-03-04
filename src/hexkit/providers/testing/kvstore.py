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

"""In-memory providers implementing the KeyValueStoreProtocol.

ATTENTION: For testing purposes only.
"""

import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, Generic

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import Dto
from hexkit.protocols.kvstore import KeyValueStoreProtocol

__all__ = [
    "InMemBytesKeyValueStore",
    "InMemDtoKeyValueStore",
    "InMemJsonKeyValueStore",
    "InMemStrKeyValueStore",
]

DEFAULT_KV_PREFIX = ""


class InMemBaseKeyValueStore(ABC, KeyValueStoreProtocol):
    """Base class for in-memory key-value stores for tests.

    Values are encoded to bytes before storage and decoded on retrieval,
    mirroring serialization behavior of production providers while keeping
    everything in memory.
    """

    _backing_store: dict[str, bytes]
    _key_prefix: str
    _repr: str

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        key_prefix: str = DEFAULT_KV_PREFIX,
        backing_store: dict[str, bytes] | None = None,
    ):
        """Yield an in-memory store instance.

        Args:
            key_prefix: Optional prefix for all keys.
            backing_store: Optional shared dict for raw encoded values.
        """
        yield cls(key_prefix=key_prefix, backing_store=backing_store)

    def __init__(
        self,
        *,
        key_prefix: str = DEFAULT_KV_PREFIX,
        backing_store: dict[str, bytes] | None = None,
    ):
        """Initialize the store with optional key prefix and shared backing store."""
        args: list[str] = []
        if key_prefix != DEFAULT_KV_PREFIX:
            args.append(f"key_prefix={key_prefix!r}")

        args_repr = " ".join(args)
        self._repr = f"{self.__class__.__name__}({args_repr})"

        self._key_prefix = key_prefix
        self._backing_store = {} if backing_store is None else backing_store

    def __repr__(self) -> str:
        return self._repr

    def _make_key(self, key: str) -> str:
        """Construct a fully-qualified key including prefix."""
        return f"{self._key_prefix}{key}"

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        self._backing_store.pop(self._make_key(key), None)

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        return self._make_key(key) in self._backing_store

    @abstractmethod
    async def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes before writing it to memory."""

    @abstractmethod
    async def _decode_value(self, raw: bytes) -> Any:
        """Decode bytes read from memory back to the target type."""

    async def get(self, key: str, default=None):
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        raw = self._backing_store.get(self._make_key(key))
        if raw is None:
            return default
        return await self._decode_value(raw)

    async def set(self, key: str, value) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        encoded_value = await self._encode_value(value)
        self._backing_store[self._make_key(key)] = encoded_value


class InMemBytesKeyValueStore(InMemBaseKeyValueStore):
    """In-memory KVStore provider for binary (bytes) data."""

    async def _encode_value(self, value: bytes) -> bytes:
        """Bytes values are stored as-is."""
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        return value

    async def _decode_value(self, raw: bytes) -> bytes:
        """Bytes values are retrieved as-is."""
        return raw


class InMemStrKeyValueStore(InMemBaseKeyValueStore):
    """In-memory KVStore provider for string data."""

    async def _encode_value(self, value: str) -> bytes:
        """Encode string to UTF-8 bytes."""
        if not isinstance(value, str):
            raise TypeError("Value must be of type str")
        return value.encode("utf-8")

    async def _decode_value(self, raw: bytes) -> str:
        """Decode UTF-8 bytes to string."""
        return raw.decode("utf-8")


class InMemJsonKeyValueStore(InMemBaseKeyValueStore):
    """In-memory KVStore provider for JSON objects."""

    async def _encode_value(self, value: JsonObject) -> bytes:
        """Serialize JSON object to UTF-8 encoded JSON string."""
        if not isinstance(value, dict):
            raise TypeError("Value must be a dict representing a JSON object")
        return json.dumps(value).encode("utf-8")

    async def _decode_value(self, raw: bytes) -> JsonObject:
        """Deserialize UTF-8 encoded JSON string to JSON object."""
        return json.loads(raw.decode("utf-8"))


class InMemDtoKeyValueStore(InMemBaseKeyValueStore, Generic[Dto]):
    """In-memory KVStore provider for Pydantic model (DTO) data."""

    _dto_model: type[Dto]

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        key_prefix: str = DEFAULT_KV_PREFIX,
        backing_store: dict[str, bytes] | None = None,
        dto_model: type[Dto] | None = None,
    ):
        """Yield an in-memory DTO store instance."""
        if dto_model is None:
            raise ValueError("The `dto_model` argument is required")
        yield cls(
            dto_model=dto_model,
            key_prefix=key_prefix,
            backing_store=backing_store,
        )

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        key_prefix: str = DEFAULT_KV_PREFIX,
        backing_store: dict[str, bytes] | None = None,
    ):
        """Initialize with DTO model type and optional storage settings."""
        super().__init__(
            key_prefix=key_prefix,
            backing_store=backing_store,
        )
        self._dto_model = dto_model
        self._repr = (
            f"{self.__class__.__name__}(dto_model={dto_model.__name__}"
            + (f" key_prefix={key_prefix!r}" if key_prefix != DEFAULT_KV_PREFIX else "")
            + ")"
        )

    async def _encode_value(self, value: Dto) -> bytes:
        """Serialize DTO to JSON bytes."""
        if not isinstance(value, self._dto_model):
            raise TypeError(f"Value must be of type {self._dto_model.__name__}")
        return value.model_dump_json().encode("utf-8")

    async def _decode_value(self, raw: bytes) -> Dto:
        """Deserialize JSON bytes to DTO."""
        return self._dto_model.model_validate_json(raw)

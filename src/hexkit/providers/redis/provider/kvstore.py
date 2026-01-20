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

"""Redis-based provider implementing the KVStoreProtocol."""

import json
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from typing import Any, Generic

from redis.asyncio import Redis

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import Dto
from hexkit.protocols.kvstore import KeyValueStoreProtocol
from hexkit.providers.redis.config import RedisConfig
from hexkit.providers.redis.provider.client import ConfiguredRedisClient

__all__ = [
    "RedisBytesKeyValueStore",
    "RedisDtoKeyValueStore",
    "RedisJsonKeyValueStore",
    "RedisStrKeyValueStore",
]

DEFAULT_KV_PREFIX = ""


class RedisBaseKeyValueStore(ABC, KeyValueStoreProtocol):
    """Base class for Redis key-value stores providing common functionality.

    Redis natively stores bytes, so this base class operates on bytes and
    subclasses provide encoding/decoding for their specific value types.
    """

    _args: str
    _client: Redis
    _key_prefix: str

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: RedisConfig,
        key_prefix: str = DEFAULT_KV_PREFIX,
        **kwargs,
    ):
        """Yields a store instance with the provided configuration.

        The Store will use the specified key prefix for all keys,
        allowing logical separation of different stores.

        The client connection is established and closed automatically.

        Args:
            config: Redis-specific config parameters.
            key_prefix: Optional prefix for all keys (e.g., "myapp:users:").
            **kwargs: Additional arguments specific to subclasses.
        """
        async with ConfiguredRedisClient(config) as client:
            yield cls(
                client=client,
                config=config,
                key_prefix=key_prefix,
                **kwargs,
            )

    def __init__(
        self,
        *,
        client: Redis,
        config: RedisConfig,
        key_prefix: str = DEFAULT_KV_PREFIX,
        **kwargs,
    ):
        """Initialize the provider with configuration and key prefix.

        Args:
            client: An instance of an async Redis client.
            config: Redis-specific config parameters.
            key_prefix: Prefix for all keys to enable logical separation.
            **kwargs: Additional arguments specific to subclasses.
        """
        args = [str(config)]
        if key_prefix != DEFAULT_KV_PREFIX:
            args.append(f"key_prefix={key_prefix!r}")
        for key, val in kwargs.items():
            with suppress(AttributeError):
                val = val.__name__  # shot repr e.g. for model classes
            args.append(f"{key}={val}")
        self._repr = f"{self.__class__.__name__}({' '.join(args)})"
        self._client = client
        self._key_prefix = key_prefix

    def __repr__(self) -> str:
        return self._repr

    def _make_key(self, key: str) -> str:
        """Construct the full Redis key with prefix."""
        return f"{self._key_prefix}{key}"

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        await self._client.delete(self._make_key(key))

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        return bool(await self._client.exists(self._make_key(key)))

    @abstractmethod
    async def _encode_value(self, value: Any) -> bytes:
        """Encode a value to bytes before writing it to Redis."""
        pass

    @abstractmethod
    async def _decode_value(self, value: bytes) -> Any:
        """Decode bytes read from Redis to the target type."""
        pass

    async def get(self, key: str, default=None):
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        raw_value = await self._client.get(self._make_key(key))
        if raw_value is None:
            return default
        return await self._decode_value(raw_value)

    async def set(self, key: str, value) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        encoded_value = await self._encode_value(value)
        await self._client.set(self._make_key(key), encoded_value)


class RedisBytesKeyValueStore(RedisBaseKeyValueStore):
    """Redis-specific KVStore provider for binary (bytes) data.

    Since Redis natively stores bytes, this is the most direct mapping
    with identity transformations.
    """

    async def _encode_value(self, value: bytes) -> bytes:
        """Bytes values are stored as-is."""
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        return value

    async def _decode_value(self, value: bytes) -> bytes:
        """Bytes values are retrieved as-is."""
        return value


class RedisStrKeyValueStore(RedisBaseKeyValueStore):
    """Redis-specific KVStore provider for string data.

    Strings are encoded to UTF-8 bytes for storage.
    """

    async def _encode_value(self, value: str) -> bytes:
        """Encode string to UTF-8 bytes."""
        if not isinstance(value, str):
            raise TypeError("Value must be of type str")
        return value.encode("utf-8")

    async def _decode_value(self, value: bytes) -> str:
        """Decode UTF-8 bytes to string."""
        return value.decode("utf-8")


class RedisJsonKeyValueStore(RedisBaseKeyValueStore):
    """Redis-specific KVStore provider for JSON data.

    JSON objects are serialized to JSON strings and then encoded to UTF-8 bytes.
    """

    async def _encode_value(self, value: JsonObject) -> bytes:
        """Serialize JSON object to UTF-8 encoded JSON string."""
        if not isinstance(value, dict):
            raise TypeError("Value must be a dict representing a JSON object")
        return json.dumps(value).encode("utf-8")

    async def _decode_value(self, value: bytes) -> JsonObject:
        """Deserialize UTF-8 encoded JSON string to JSON object."""
        return json.loads(value.decode("utf-8"))


class RedisDtoKeyValueStore(RedisBaseKeyValueStore, Generic[Dto]):
    """Redis-specific KVStore provider for Pydantic model (DTO) data.

    This store allows arbitrary DTOs (Pydantic models) to be stored as values,
    with the DTO model passed in the constructor. DTOs are serialized using
    Pydantic's model_dump, converted to JSON, and stored as UTF-8 bytes.
    """

    _dto_model: type[Dto]

    def __init__(
        self,
        *,
        client: Redis,
        config: RedisConfig,
        dto_model: type[Dto],
        key_prefix: str,
    ):
        """Initialize the provider with configuration, model type, and key prefix.

        Args:
            client: An instance of an async Redis client.
            config: Redis-specific config parameters.
            dto_model: The Pydantic model type to support for values.
            key_prefix: Prefix for all keys to enable logical separation.
        """
        super().__init__(
            client=client,
            config=config,
            key_prefix=key_prefix,
            dto_model=dto_model,
        )
        self._dto_model = dto_model

    async def _encode_value(self, value: Dto) -> bytes:
        """Transform DTO to JSON and encode to UTF-8 bytes."""
        if not isinstance(value, self._dto_model):
            raise TypeError(f"Value must be of type {self._dto_model.__name__}")
        return json.dumps(value.model_dump()).encode("utf-8")

    async def _decode_value(self, value: bytes) -> Dto:
        """Decode UTF-8 bytes, parse JSON, and validate as DTO."""
        value_dict = json.loads(value.decode("utf-8"))
        return self._dto_model.model_validate(value_dict)

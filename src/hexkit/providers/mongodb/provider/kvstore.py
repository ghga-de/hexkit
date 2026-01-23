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

"""MongoDB-based provider implementing the KVStoreProtocol."""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from typing import Any, Generic

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import Dto
from hexkit.protocols.kvstore import KeyValueStoreProtocol
from hexkit.providers.mongodb.config import MongoDbConfig
from hexkit.providers.mongodb.provider.client import ConfiguredMongoClient

__all__ = [
    "MongoDbBytesKeyValueStore",
    "MongoDbDtoKeyValueStore",
    "MongoDbJsonKeyValueStore",
    "MongoDbStrKeyValueStore",
]


DEFAULT_KV_COLLECTION_NAME = "kvstore"


class MongoDbBaseKeyValueStore(ABC, KeyValueStoreProtocol):
    """Base class for MongoDB key-value stores providing common functionality."""

    _args: str
    _collection: AsyncCollection

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: MongoDbConfig,
        collection_name: str = DEFAULT_KV_COLLECTION_NAME,
        **kwargs,
    ):
        """Yields a store instance with the provided configuration.

        The Store will use the specified collection name to store key-value pairs,
        by default the collection will be named 'kvstore'.

        The client connection is established and closed automatically.
        """
        async with ConfiguredMongoClient(config=config) as client:
            yield cls(
                client=client,
                config=config,
                collection_name=collection_name,
                **kwargs,
            )

    def __init__(
        self,
        *,
        client: AsyncMongoClient,
        config: MongoDbConfig,
        collection_name: str = DEFAULT_KV_COLLECTION_NAME,
        **kwargs,
    ):
        """Initialize the provider with configuration and collection name.

        Args:
            client: An instance of an async MongoDB client.
            config: MongoDB-specific config parameters.
            collection_name: Name of the collection to hold the key-value pairs.
            **kwargs: Additional arguments specific to subclasses.
        """
        args = [str(config)]
        if collection_name != DEFAULT_KV_COLLECTION_NAME:
            args.append(f"collection_name={collection_name!r}")
        for key, val in kwargs.items():
            with suppress(AttributeError):
                val = val.__name__  # shot repr e.g. for model classes
            args.append(f"{key}={val}")
        self._repr = f"{self.__class__.__name__}({' '.join(args)})"
        db = client.get_database(config.db_name)
        self._collection = db[collection_name]

    def __repr__(self) -> str:
        return self._repr

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        await self._collection.delete_one({"_id": key})

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        document = await self._collection.find_one({"_id": key}, {"_id": 1})
        return document is not None

    @abstractmethod
    async def _encode_value(self, value: Any) -> Any:
        """Encode a value before writing it to MongoDB."""
        pass

    @abstractmethod
    async def _decode_value(self, value: Any) -> Any:
        """Decode a value read from MongoDB."""
        pass

    async def get(self, key: str, default=None):
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        document = await self._collection.find_one({"_id": key})
        if document is None:
            return default
        value = document.get("value")
        if value is None:
            return default
        return await self._decode_value(value)

    async def set(self, key: str, value):
        """Set the value for the given key.

        Note that values cannot be None.
        """
        encoded_value = await self._encode_value(value)
        document = {"_id": key, "value": encoded_value}
        await self._collection.replace_one({"_id": key}, document, upsert=True)


class MongoDbJsonKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KVStore provider for JSON data."""

    async def _encode_value(self, value: JsonObject) -> JsonObject:
        """JSON values are stored as-is."""
        if not isinstance(value, dict):
            raise TypeError("Value must be a dict representing a JSON object")
        return value

    async def _decode_value(self, value: JsonObject) -> JsonObject:
        """JSON values are retrieved as-is."""
        return value


class MongoDbStrKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KVStore provider for string data."""

    async def _encode_value(self, value: str) -> str:
        """String values are stored as-is."""
        if not isinstance(value, str):
            raise TypeError("Value must be of type str")
        return value

    async def _decode_value(self, value: str) -> str:
        """String values are retrieved as-is."""
        return value


class MongoDbBytesKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KVStore provider for binary (bytes) data."""

    async def _encode_value(self, value: bytes) -> bytes:
        """Bytes values are stored as-is."""
        if not isinstance(value, bytes):
            raise TypeError("Value must be of type bytes")
        return value

    async def _decode_value(self, value: bytes) -> bytes:
        """Bytes values are retrieved as-is."""
        return value


class MongoDbDtoKeyValueStore(MongoDbBaseKeyValueStore, Generic[Dto]):
    """MongoDB specific KVStore provider for Pydantic model (DTO) data.

    This store allows arbitrary DTOs (Pydantic models) to be stored as values,
    with the DTO model passed in the constructor.
    """

    _dto_model: type[Dto]

    def __init__(
        self,
        *,
        client: AsyncMongoClient,
        config: MongoDbConfig,
        dto_model: type[Dto],
        collection_name: str,
    ):
        """Initialize the provider with configuration, model type, and collection name.

        Args:
            client: An instance of an async MongoDB client.
            config: MongoDB-specific config parameters.
            dto_model: The Pydantic model type to support for values.
            collection_name: Name of the collection to hold the key-value pairs.
        """
        super().__init__(
            client=client,
            config=config,
            collection_name=collection_name,
            dto_model=dto_model,
        )
        self._dto_model = dto_model

    async def _encode_value(self, value: Dto) -> dict:
        """Transform DTO to dictionary for storage."""
        if not isinstance(value, self._dto_model):
            raise TypeError(f"Value must be of type {self._dto_model.__name__}")
        return value.model_dump()

    async def _decode_value(self, value: dict) -> Dto:
        """Transform dictionary from storage back to DTO."""
        return self._dto_model.model_validate(value)

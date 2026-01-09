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
from contextlib import asynccontextmanager
from typing import Generic

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
        collection_name: str = "kvstore",
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
        collection_name: str,
        **kwargs,
    ):
        """Initialize the provider with configuration and collection name.

        Args:
            client: An instance of an async MongoDB client.
            config: MongoDB-specific config parameters.
            collection_name: Name of the collection to hold the key-value pairs.
            **kwargs: Additional arguments specific to subclasses.
        """
        args_dict = {"config": config, "collection_name": collection_name}
        args_dict.update(kwargs)
        self._args = repr(args_dict)
        db = client.get_database(config.db_name)
        self._collection = db[collection_name]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._args})"

    @abstractmethod
    async def get(self, key: str, default=None):
        """Retrieve the value for the given key."""
        pass

    @abstractmethod
    async def set(self, key: str, value):
        """Set the value for the given key."""
        pass

    async def delete(self, key: str) -> None:
        """Delete the value for the given key.

        Does nothing if there is no such value.
        """
        await self._collection.delete_one({"_id": key})

    async def exists(self, key: str) -> bool:
        """Check if the given key exists."""
        document = await self._collection.find_one({"_id": key}, {"_id": 1})
        return document is not None


class MongoDbJsonKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KV store provider for JSON data."""

    async def get(
        self, key: str, default: JsonObject | None = None
    ) -> JsonObject | None:
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        document = await self._collection.find_one({"_id": key})
        if document is None:
            return default
        value = document.get("value")
        return value if value is not None else default

    async def set(self, key: str, value: JsonObject) -> None:
        """Set the value for the given key.

        Note that JsonObjects cannot be None.
        """
        document = {"_id": key, "value": value}
        await self._collection.replace_one({"_id": key}, document, upsert=True)


class MongoDbStrKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KV store provider for string data."""

    async def get(self, key: str, default: str | None = None) -> str | None:
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        document = await self._collection.find_one({"_id": key})
        if document is None:
            return default
        value = document.get("value")
        return value if value is not None else default

    async def set(self, key: str, value: str) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        document = {"_id": key, "value": value}
        await self._collection.replace_one({"_id": key}, document, upsert=True)


class MongoDbBytesKeyValueStore(MongoDbBaseKeyValueStore):
    """MongoDB specific KV store provider for binary (bytes) data."""

    async def get(self, key: str, default: bytes | None = None) -> bytes | None:
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        document = await self._collection.find_one({"_id": key})
        if document is None:
            return default
        value = document.get("value")
        return value if value is not None else default

    async def set(self, key: str, value: bytes) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        document = {"_id": key, "value": value}
        await self._collection.replace_one({"_id": key}, document, upsert=True)


class MongoDbDtoKeyValueStore(MongoDbBaseKeyValueStore, Generic[Dto]):
    """MongoDB specific KV store provider for Pydantic model (DTO) data.

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

    async def get(self, key: str, default: Dto | None = None) -> Dto | None:
        """Retrieve the value for the given key.

        Returns the specified default value if there is no such value in the store.
        """
        document = await self._collection.find_one({"_id": key})
        if document is None:
            return default
        value_dict = document.get("value")
        if value_dict is None:
            return default
        return self._dto_model.model_validate(value_dict)

    async def set(self, key: str, value: Dto) -> None:
        """Set the value for the given key.

        Note that values cannot be None.
        """
        value_dict = value.model_dump()
        document = {"_id": key, "value": value_dict}
        await self._collection.replace_one({"_id": key}, document, upsert=True)

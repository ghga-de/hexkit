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

from contextlib import asynccontextmanager

from pymongo import AsyncDatabase, AsyncMongoClient

from hexkit.protocols.kvstore import KeyValueStoreProtocol
from hexkit.providers.mongodb.config import MongoDbConfig
from hexkit.providers.mongodb.provider.client import ConfiguredMongoClient


class MongoDbJsonKeyValueStore(KeyValueStoreProtocol):
    """MongoDB specific KV store provider for JSON data."""

    _config: MongoDbConfig
    _collection_name: str
    _db: AsyncDatabase

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: MongoDbConfig,
        collection_name: str = "kvstore",
    ):
        """Yields a MongoDbKeyValueStore instance with the provided configuration.

        The Store will use the specified collection name to store key-value pairs,
        by default the collection will be named 'kvstore'.

        The client connection is established and closed automatically.
        """
        async with ConfiguredMongoClient(config=config) as client:
            yield cls(client=client, config=config, collection_name=collection_name)

    def __init__(
        self,
        *,
        client: AsyncMongoClient,
        config: MongoDbConfig,
        collection_name: str,
    ):
        """Initialize the provider with configuration and collection name.

        Args:
            client: An instance of an async MongoDB client.
            config: MongoDB-specific config parameters.
            collection_name: Name of the collection to hold the key-value pairs.
        """
        self._config = config
        self._collection_name = collection_name
        self._db = client.get_database(self._config.db_name)

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"{self.__class__.__qualname__}("
            f"config={repr(self._config)},"
            f" collection_name={self._collection_name})"
        )

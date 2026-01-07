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

"""Client used for the MongoDB providers."""

from typing import TypeVar

from pymongo import AsyncMongoClient, MongoClient

from hexkit.providers.mongodb.config import MongoDbConfig

ClientType = TypeVar("ClientType", AsyncMongoClient, MongoClient)

__all__ = ["ConfiguredMongoClient"]


class ConfiguredMongoClient:
    """A context manager for a configured MongoDB client, sync or async.

    Usage:
        ```python
        from hexkit.providers.mongodb import MongoDbConfig, ConfiguredMongoClient

        with ConfiguredMongoClient(config=MongoDbConfig(...)) as client:
            # Use the client here
            pass

        async with ConfiguredMongoClient(config=MongoDbConfig(...)) as client:
            # Use the async client here
            pass

        # Client is automatically closed after exiting the context manager
    """

    config: MongoDbConfig

    def __init__(self, *, config: MongoDbConfig):
        self._config = config

    async def __aenter__(self) -> AsyncMongoClient:
        """Enter a context manager and return the _asynchronous_ MongoDB client."""
        self._async_client = self.get_client(
            config=self._config, client_cls=AsyncMongoClient
        )
        return self._async_client

    async def __aexit__(self, exc_type_, exc_value, exc_tb):
        """Close the async MongoDB client."""
        await self._async_client.close()

    def __enter__(self) -> MongoClient:
        """Enter a context manager and return the _synchronous_ MongoDB client."""
        self._client = self.get_client(config=self._config, client_cls=MongoClient)
        return self._client

    def __exit__(self, exc_type_, exc_value, exc_tb):
        """Close the synchronous MongoDB client."""
        self._client.close()

    @classmethod
    def get_client(
        cls,
        *,
        config: MongoDbConfig,
        client_cls: type[ClientType],
    ) -> ClientType:
        """Creates a configured MongoDB client based on the provided configuration,
        with the timeout, uuid representation, and timezone awareness set.

        *Does not* automatically close the client!

        Args:
            config: MongoDB-specific configuration parameters.
            client_cls: The class of the MongoDB client to instantiate.

        Returns:
            A MongoDB client instance configured with the provided parameters.
        """
        timeout_ms = (
            int(config.mongo_timeout * 1000)
            if config.mongo_timeout is not None
            else None
        )

        return client_cls(
            str(config.mongo_dsn.get_secret_value()),
            timeoutMS=timeout_ms,
            uuidRepresentation="standard",
            tz_aware=True,
        )

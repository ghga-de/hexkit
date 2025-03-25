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
#

"""Utilities for testing code that uses the MongoDbDaoFactory provider.

Please note, only use for testing purposes.
"""

from collections.abc import Generator
from dataclasses import dataclass
from typing import Optional, Union

import pytest
from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout, OperationFailure
from testcontainers.mongodb import MongoDbContainer

from hexkit.custom_types import PytestScope
from hexkit.providers.mongodb.provider import MongoDbConfig, MongoDbDaoFactory

MONGODB_IMAGE = "mongo:7.0.15"

__all__ = [
    "MONGODB_IMAGE",
    "MongoClient",
    "MongoDbConfig",
    "MongoDbContainerFixture",
    "MongoDbDaoFactory",
    "MongoDbFixture",
    "clean_mongodb_fixture",
    "get_clean_mongodb_fixture",
    "get_mongodb_container_fixture",
    "get_persistent_mongodb_fixture",
    "mongodb_container_fixture",
    "mongodb_fixture",
    "persistent_mongodb_fixture",
]


@dataclass(frozen=True)
class MongoDbFixture:
    """A fixture with utility methods for tests that use MongoDB"""

    client: MongoClient
    config: MongoDbConfig
    dao_factory: MongoDbDaoFactory

    def empty_collections(
        self,
        *,
        collections: Optional[Union[str, list[str]]] = None,
        exclude_collections: Optional[Union[str, list[str]]] = None,
    ):
        """Drop the given mongodb collection(s) in the database.

        If no collections are specified, all collections will be dropped.
        You can also specify collection(s) that should be excluded
        from the operation, i.e. collections that should be kept.
        """
        db_name = self.config.db_name
        try:
            if collections is None:
                collections = self.client[db_name].list_collection_names()
            elif isinstance(collections, str):
                collections = [collections]
            if exclude_collections is None:
                exclude_collections = []
            elif isinstance(exclude_collections, str):
                exclude_collections = [exclude_collections]
            excluded_collections = set(exclude_collections)
            for collection in collections:
                if collection not in excluded_collections:
                    self.client[db_name].drop_collection(collection)
        except (ExecutionTimeout, OperationFailure) as error:
            raise RuntimeError(
                f"Could not drop collection(s) of Mongo database {db_name}"
            ) from error


class MongoDbContainerFixture(MongoDbContainer):
    """MongoDB test container with MongoDB configuration."""

    mongodb_config: MongoDbConfig


def _mongodb_container_fixture() -> Generator[MongoDbContainerFixture, None, None]:
    """Fixture function for getting a running MongoDB test container."""
    with MongoDbContainerFixture(image=MONGODB_IMAGE) as mongodb_container:
        mongodb_config = MongoDbConfig(
            mongo_dsn=mongodb_container.get_connection_url(),
            db_name="test",
        )
        mongodb_container.mongodb_config = mongodb_config
        yield mongodb_container


def get_mongodb_container_fixture(
    scope: PytestScope = "session", name: str = "mongodb_container"
):
    """Get a MongoDB test container fixture with desired scope and name.

    By default, the session scope is used for MongoDB test containers.
    """
    return pytest.fixture(_mongodb_container_fixture, scope=scope, name=name)


mongodb_container_fixture = get_mongodb_container_fixture()


def _persistent_mongodb_fixture(
    mongodb_container: MongoDbContainerFixture,
) -> Generator[MongoDbFixture, None, None]:
    """Fixture function that gets a persistent MongoDB fixture.

    The state of the MongoDB is not cleaned up by the function.
    """
    config = mongodb_container.mongodb_config
    dao_factory = MongoDbDaoFactory(config=config)
    client = mongodb_container.get_connection_client()
    yield MongoDbFixture(
        client=client,
        config=config,
        dao_factory=dao_factory,
    )

    client.close()


def get_persistent_mongodb_fixture(
    scope: PytestScope = "function", name: str = "mongodb"
):
    """Get a MongoDB fixture with desired scope and name.

    The state of the MongoDB test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying MongoDB test container.
    """
    return pytest.fixture(_persistent_mongodb_fixture, scope=scope, name=name)


persistent_mongodb_fixture = get_persistent_mongodb_fixture()


def _clean_mongodb_fixture(
    mongodb_container: MongoDbContainerFixture,
) -> Generator[MongoDbFixture, None, None]:
    """Fixture function that gets a clean MongoDB fixture.

    The clean state is achieved by emptying all MongoDB collections upfront.
    """
    for mongodb_fixture in _persistent_mongodb_fixture(mongodb_container):
        mongodb_fixture.empty_collections()
        yield mongodb_fixture


def get_clean_mongodb_fixture(scope: PytestScope = "function", name: str = "mongodb"):
    """Get a MongoDB fixture with desired scope and name.

    The state of the MongoDB is reset by emptying all collections before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying MongoDB test container.
    """
    return pytest.fixture(_clean_mongodb_fixture, scope=scope, name=name)


mongodb_fixture = clean_mongodb_fixture = get_clean_mongodb_fixture()

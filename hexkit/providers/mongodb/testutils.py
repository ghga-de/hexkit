# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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


from dataclasses import dataclass
from typing import Generator, Optional, Union

from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout, OperationFailure
from testcontainers.mongodb import MongoDbContainer

from hexkit.providers.mongodb.provider import MongoDbConfig, MongoDbDaoFactory


@dataclass(frozen=True)
class MongoDbFixture:
    """Yielded by the `mongodb_fixture` function"""

    client: MongoClient
    config: MongoDbConfig
    dao_factory: MongoDbDaoFactory

    def empty_collections(
        self,
        exclude_collections: Optional[Union[str, list[str]]] = None,
    ):
        """Drop all mongodb collections in the database.

        You can also specify collection(s) that should be excluded
        from the operation, i.e. collections that should be kept.
        """
        db_name = self.config.db_name
        if exclude_collections is None:
            exclude_collections = []
        if isinstance(exclude_collections, str):
            exclude_collections = [exclude_collections]
        excluded_collections = set(exclude_collections)
        try:
            collection_names = self.client[db_name].list_collection_names()
            for collection_name in collection_names:
                if collection_name not in excluded_collections:
                    self.client[db_name].drop_collection(collection_name)
        except (ExecutionTimeout, OperationFailure) as error:
            raise RuntimeError(
                f"Could not drop collection(s) of Mongo database {db_name}"
            ) from error
        for collection_name in self.client[db_name].list_collection_names():
            self.client[db_name].drop_collection(collection_name)


def config_from_mongodb_container(container: MongoDbContainer) -> MongoDbConfig:
    """Prepares a MongoDbConfig from an instance of a MongoDbContainer container."""

    db_connection_str = container.get_connection_url()
    return MongoDbConfig(db_connection_str=db_connection_str, db_name="test")


def mongodb_fixture_function() -> Generator[MongoDbFixture, None, None]:
    """
    Pytest fixture for tests depending on the MongoDbDaoFactory DAO.
    Obtained via get_fixture in hexkit.providers.testing.fixtures.get_fixture
    """

    with MongoDbContainer(image="mongo:6.0.3") as mongodb:
        config = config_from_mongodb_container(mongodb)
        dao_factory = MongoDbDaoFactory(config=config)
        client = mongodb.get_connection_client()

        yield MongoDbFixture(
            client=client,
            config=config,
            dao_factory=dao_factory,
        )

        client.close()

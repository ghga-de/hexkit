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
from typing import Generator

from pytest_asyncio.plugin import _ScopeName
from testcontainers.mongodb import MongoDbContainer

from hexkit.providers.mongodb.provider import MongoDbConfig, MongoDbDaoFactory
from hexkit.providers.testing.fixture_factory import produce_fixture


@dataclass(frozen=True)
class MongoDbFixture:
    """Yielded by the `mongodb_fixture` function"""

    config: MongoDbConfig
    dao_factory: MongoDbDaoFactory


def config_from_mongodb_container(container: MongoDbContainer) -> MongoDbConfig:
    """Prepares a MongoDbConfig from an instance of a MongoDbContainer container."""

    db_connection_str = container.get_connection_url()
    return MongoDbConfig(db_connection_str=db_connection_str, db_name="test")


def mongodb_fixture_function() -> Generator[MongoDbFixture, None, None]:
    """
    Pytest fixture for tests depending on the MongoDbDaoFactory DAO.
    Obtained via get_mongodb_fixture
    """

    with MongoDbContainer(image="mongo:6.0.3") as mongodb:
        config = config_from_mongodb_container(mongodb)
        dao_factory = MongoDbDaoFactory(config=config)

        yield MongoDbFixture(config=config, dao_factory=dao_factory)


def get_mongodb_fixture(scope: _ScopeName = "function"):
    """Return a scoped mongodb fixture"""
    return produce_fixture(mongodb_fixture_function, scope)


mongodb_fixture = get_mongodb_fixture()  # don't break old references to mongodb_fixture

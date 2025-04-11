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

"""Utilities for testing code that uses the MongoKafkaDaoPublisherFactory provider.

Please note, only use for testing purposes.
"""

from typing import NamedTuple

import pytest

from hexkit.providers.akafka.testutils import KafkaContainerFixture, KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbContainerFixture, MongoDbFixture
from hexkit.providers.mongokafka import MongoKafkaConfig

__all__ = [
    "MongoKafkaConfig",
    "MongoKafkaFixture",
    "mongo_kafka_config_fixture",
    "mongo_kafka_fixture",
]


@pytest.fixture(name="mongo_kafka_config", scope="session")
def mongo_kafka_config_fixture(
    mongodb_container: MongoDbContainerFixture, kafka_container: KafkaContainerFixture
) -> MongoKafkaConfig:
    """Fixture to get the combined Mongo and Kafka configuration."""
    return MongoKafkaConfig(
        **mongodb_container.mongodb_config.model_dump(),
        **kafka_container.kafka_config.model_dump(),
    )


class MongoKafkaFixture(NamedTuple):
    """Combined MongoDB and Kafka fixture"""

    mongodb: MongoDbFixture
    kafka: KafkaFixture
    config: MongoKafkaConfig


@pytest.fixture(name="mongo_kafka", scope="function")
def mongo_kafka_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> MongoKafkaFixture:
    """Get a MongoKafka fixture with desired scope and name.

    The state of the MongoDb and Kafka is reset by emptying all MongoDB collections
    and clearing all Kafka topics before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying test containers.
    """
    config = MongoKafkaConfig(
        **mongodb.config.model_dump(),
        **kafka.config.model_dump(),
    )
    return MongoKafkaFixture(mongodb, kafka, config)

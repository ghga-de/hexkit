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

"""Testing of the MongoKafka testutils."""

import pytest

from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)
from hexkit.providers.mongokafka.testutils import (
    MongoKafkaConfig,
    MongoKafkaFixture,
    mongo_kafka_config_fixture,  # noqa: F401
    mongo_kafka_fixture,  # noqa: F401
)


@pytest.mark.asyncio()
async def test_mongo_kafka_config_fixture(
    mongo_kafka_config: MongoKafkaConfig, mongo_kafka: MongoKafkaFixture
):
    """Test the MongoKafkaConfig fixture together with the MongoKafkaFixture."""
    assert isinstance(mongo_kafka_config, MongoKafkaConfig)
    assert isinstance(mongo_kafka.mongodb, MongoDbFixture)
    assert isinstance(mongo_kafka.kafka, KafkaFixture)
    assert isinstance(mongo_kafka.config, MongoKafkaConfig)
    assert mongo_kafka_config == mongo_kafka.config

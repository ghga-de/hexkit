# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Integration tests for the PersistentKafkaPublisher"""

# Test `published` is False before publish and True afterward
# Test sorting by `created`
# Test `publish_pending` and `republish` when DB is empty
# Test collection name assignment
# Test with error during publish (make sure dto left as unpublished)
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from hexkit.providers.mongodb.provider import MongoDbDaoFactory

pytestmark = pytest.mark.asyncio()
from hexkit.correlation import set_correlation_id
from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)
from hexkit.providers.mongodb.provider import document_to_dto
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)
from hexkit.providers.mongokafka.provider import (
    MongoKafkaConfig,
    PersistentKafkaPublisher,
)
from hexkit.providers.mongokafka.provider.persistent_pub import PersistentKafkaEvent

TEST_TOPIC = "my-topic"
TEST_TYPE = "my_type"
TEST_PAYLOAD = {"some": "payload"}
TEST_KEY = "somekey123"
TEST_CORRELATION_ID = "9ef5956f-be9c-427a-ab4a-42ae1e231c86"


async def test_basic_publish(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test the error-free path of publishing with the PersistentKafkaPublisher.

    This test verifies that;
    - the event is stored in the database
    - the event is marked as published after publishing completes
    - the timestamp is created correctly
    """
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    expected_db_event = {
        "topic": TEST_TOPIC,
        "type_": TEST_TYPE,
        "payload": TEST_PAYLOAD,
        "key": TEST_KEY,
        "headers": {},
        "correlation_id": TEST_CORRELATION_ID,
        "published": True,
    }

    # First, make sure the collection is empty
    db = mongodb.client.get_database(config.db_name)
    collection = db[collection_name]
    assert not collection.find().to_list()

    # Publish an event, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
        ) as persistent_publisher,
        kafka.record_events(in_topic=TEST_TOPIC, capture_headers=True) as recorder,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        await persistent_publisher.publish(
            payload=TEST_PAYLOAD,
            topic=TEST_TOPIC,
            type_=TEST_TYPE,
            key=TEST_KEY,
            headers=None,
        )
    # Inspect & verify some expectations about the event that we published
    assert recorder.recorded_events
    assert len(recorder.recorded_events) == 1
    event = recorder.recorded_events[0]
    assert event.headers == {"correlation_id": TEST_CORRELATION_ID}
    assert event.key == TEST_KEY
    assert event.type_ == TEST_TYPE
    assert event.payload == TEST_PAYLOAD

    # Now get the DB collection contents (should be one document)
    docs = collection.find().to_list()
    assert len(docs) == 1
    stored_event_doc = docs[0]

    # Convert to model and dump as dict
    dto_dict = document_to_dto(
        stored_event_doc, id_field="id_", dto_model=PersistentKafkaEvent
    ).model_dump()

    # Inspect the 'created' and 'id_' fields
    timestamp = dto_dict.pop("created")
    assert datetime.now(tz=UTC) - timestamp <= timedelta(seconds=30)
    assert isinstance(dto_dict.pop("id_"), UUID)
    assert dto_dict == expected_db_event


async def test_republish(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test republishing with the PersistentKafkaPublisher.

    This test verifies that;
    - the event is stored in the database
    - the event is marked as published after publishing completes
    - the timestamp is created correctly
    """
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    # First, make sure the collection is empty
    db = mongodb.client.get_database(config.db_name)
    collection = db[collection_name]
    assert not collection.find().to_list()

    # Publish an event, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
        ) as persistent_publisher,
        kafka.record_events(in_topic=TEST_TOPIC, capture_headers=True) as recorder1,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        await persistent_publisher.publish(
            payload=TEST_PAYLOAD,
            topic=TEST_TOPIC,
            type_=TEST_TYPE,
            key=TEST_KEY,
            headers=None,
        )

    docs = collection.find().to_list()
    assert len(docs) == 1

    # Publish an event, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
        ) as persistent_publisher,
        kafka.record_events(in_topic=TEST_TOPIC, capture_headers=True) as recorder2,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        await persistent_publisher.republish()

    # Assert the published event is the same
    assert recorder1.recorded_events == recorder2.recorded_events

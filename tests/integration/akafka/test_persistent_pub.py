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

"""Integration tests for the PersistentKafkaPublisher"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from hexkit.providers.mongodb.provider import MongoDbDaoFactory
from hexkit.utils import now_utc_without_micros

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
TEST_CORRELATION_ID = UUID("9ef5956f-be9c-427a-ab4a-42ae1e231c86")


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
    assert event.headers == {"correlation_id": str(TEST_CORRELATION_ID)}
    assert event.key == TEST_KEY
    assert event.type_ == TEST_TYPE
    assert event.payload == TEST_PAYLOAD

    # Now get the DB collection contents (should be one document)
    docs = collection.find().to_list()
    assert len(docs) == 1
    stored_event_doc = docs[0]

    # Convert to model and dump as dict
    dto_dict = document_to_dto(
        stored_event_doc, id_field="id", dto_model=PersistentKafkaEvent
    ).model_dump()

    # Inspect the 'created' and 'id' fields
    timestamp = dto_dict.pop("created")
    assert datetime.now(tz=timezone.utc) - timestamp <= timedelta(seconds=30)

    # Verify that the ID is a valid UUID (no error means it's valid)
    UUID(dto_dict.pop("id"))
    assert dto_dict == expected_db_event


async def test_republish(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test republishing with the PersistentKafkaPublisher."""
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    # First, make sure the collection is empty
    db = mongodb.client.get_database(config.db_name)
    collection = db[collection_name]
    assert not collection.find().to_list()

    # Publish some events, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
        ) as persistent_publisher,
        kafka.record_events(in_topic=TEST_TOPIC, capture_headers=True) as recorder1,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        for i in range(3):
            await asyncio.sleep(0.1)  # tests run fast, so leave gap in timestamps
            await persistent_publisher.publish(
                payload={"payload": i},
                topic=TEST_TOPIC,
                type_=TEST_TYPE,
                key=TEST_KEY,
                headers=None,
            )

    docs = collection.find().to_list()
    assert len(docs) == 3

    # Republish the 3 stored events
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

    # Assert the published event is the same and in the same order
    assert recorder1.recorded_events == recorder2.recorded_events


async def test_publish_pending(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test that `publish_pending()` only publishes events that haven't been published
    yet.
    """
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    # Publish an event, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
        ) as persistent_publisher,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        async with kafka.record_events(
            in_topic=TEST_TOPIC, capture_headers=True
        ) as recorder:
            await persistent_publisher.publish(
                payload=TEST_PAYLOAD,
                topic=TEST_TOPIC,
                type_=TEST_TYPE,
                key=TEST_KEY,
                headers=None,
            )
        assert len(recorder.recorded_events) == 1

        # Insert an event manually in the database, marked as unpublished
        event = {
            "_id": "40a7a7c5-1e2f-4a1f-b053-cf918edd1b40",
            "topic": TEST_TOPIC,
            "type_": TEST_TYPE,
            "key": TEST_KEY,
            "payload": {"new": "payload"},
            "headers": {},
            "correlation_id": TEST_CORRELATION_ID,
            "created": now_utc_without_micros(),
            "published": False,
        }
        collection = mongodb.client[config.db_name][collection_name]
        collection.insert_one(event)

        # Double check that only the unpublished event was published.
        async with kafka.record_events(
            in_topic=TEST_TOPIC, capture_headers=True
        ) as recorder:
            await persistent_publisher.publish_pending()
        assert len(recorder.recorded_events) == 1
        assert recorder.recorded_events[0].payload == {"new": "payload"}

        # Check that both events in the DB now say published = True
        events = collection.find().to_list()
        assert len(events) == 2
        assert events[0]["published"] == True
        assert events[1]["published"] == True


async def test_compaction(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test that events for compacted topics get the correct ID, that they replace
    previous events with the same key, and that they can be republished.
    """
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    payload1 = TEST_PAYLOAD
    payload2 = {"some": "payload2"}
    # Publish an event, which should then be stored in the db
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
            compacted_topics={TEST_TOPIC},
        ) as persistent_publisher,
        kafka.record_events(
            in_topic=TEST_TOPIC, capture_headers=True
        ) as compact_recorder,
        kafka.record_events(
            in_topic="noncompacted_topic", capture_headers=True
        ) as noncompact_recorder,
        set_correlation_id(TEST_CORRELATION_ID),
    ):
        for payload in [payload1, payload2]:
            await persistent_publisher.publish(
                payload=payload,
                topic=TEST_TOPIC,
                type_=TEST_TYPE,
                key=TEST_KEY,
                headers=None,
            )
            await persistent_publisher.publish(
                payload=payload,
                topic="noncompacted_topic",
                type_=TEST_TYPE,
                key=TEST_KEY,
                headers=None,
            )

    assert len(compact_recorder.recorded_events) == 2
    assert len(noncompact_recorder.recorded_events) == 2

    # Verify that there is only one event saved in the DB for the compacted topic
    collection = mongodb.client[config.db_name][collection_name]
    events = collection.find().to_list()
    assert len(events) == 3
    event = events[0]
    assert event["_id"] == f"{TEST_TOPIC}:{TEST_KEY}"
    assert event["payload"] == payload2

    # Republish all events to make sure that both noncompacted events are republished,
    #  but only the one compacted event
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
            compacted_topics={TEST_TOPIC},
        ) as persistent_publisher,
        kafka.record_events(
            in_topic=TEST_TOPIC,
        ) as compact_recorder,
        kafka.record_events(
            in_topic="noncompacted_topic",
        ) as noncompact_recorder,
    ):
        await persistent_publisher.republish()

    assert len(compact_recorder.recorded_events) == 1
    assert len(noncompact_recorder.recorded_events) == 2


async def test_topics_not_stored(kafka: KafkaFixture, mongodb: MongoDbFixture):
    """Test that events are not stored in the DB if their topic is marked `topics_not_stored`."""
    config = MongoKafkaConfig(
        **kafka.config.model_dump(), **mongodb.config.model_dump()
    )
    collection_name = f"{config.service_name}PersistedEvents"
    dao_factory = MongoDbDaoFactory(config=config)

    # Publish an event to a topic marked as 'no store'
    async with (
        PersistentKafkaPublisher.construct(
            config=config,
            dao_factory=dao_factory,
            collection_name=collection_name,
            topics_not_stored={TEST_TOPIC},
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

    # Verify that the event was published but not stored in the database
    assert len(recorder.recorded_events) == 1
    collection = mongodb.client[config.db_name][collection_name]
    assert not collection.find().to_list()


async def test_conflicting_args():
    """Test arg validation for the `compacted_topics` and `topics_not_stored` parameters."""
    with pytest.raises(ValueError) as err:
        async with PersistentKafkaPublisher.construct(
            config=Mock(),
            dao_factory=AsyncMock(),
            collection_name="test_collection",
            compacted_topics={"compacted", "conflict1", "conflict2"},
            topics_not_stored={"conflict1", "conflict2", "no_store"},
        ):
            assert False  # Should not get here

    msg = (
        "Values for `topics_not_stored` and `compacted_topics` must be exclusive."
        + " Please review the following values: conflict1, conflict2."
    )
    assert err.value.args[0] == msg

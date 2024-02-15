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

"""Testing of the Kafka testutils."""
import json
from collections.abc import Sequence

import pytest
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin.new_topic import NewTopic

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.provider import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    RecordedEvent,
    ValidationError,
    kafka_fixture,  # noqa: F401
)

TEST_TYPE = "test_type"


def make_payload(msg: str) -> JsonObject:
    """Return a test payload with a specified string."""
    return {"test_msg": msg}


class DummyTranslator(EventSubscriberProtocol):
    """An event subscriber translator that simply tracks what it's consumed."""

    def __init__(self, topics_of_interest: list[str]):
        self.topics_of_interest = topics_of_interest
        self.types_of_interest = [TEST_TYPE]
        self.consumed: dict[str, list[JsonObject]] = {
            topic: [] for topic in topics_of_interest
        }

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii
    ):
        self.consumed[topic].append(payload)


def test_delete_topics_specific(kafka_fixture: KafkaFixture):  # noqa: F811
    """Make sure the reset function works"""
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_fixture.kafka_servers)
    new_topics = [
        NewTopic(name="test", num_partitions=1, replication_factor=1),
        NewTopic(name="test2", num_partitions=1, replication_factor=1),
    ]
    admin_client.create_topics(new_topics)

    initial_topics = admin_client.list_topics()

    assert "test" in initial_topics

    # delete that topic
    kafka_fixture.delete_topics("test")

    # make sure it got deleted
    final_topics = admin_client.list_topics()
    admin_client.close()
    assert "test" not in final_topics
    assert "test2" in final_topics


def test_delete_topics_all(kafka_fixture: KafkaFixture):  # noqa: F811
    """Test topic deletion without specifying parameters"""
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_fixture.kafka_servers)

    new_topics = [
        NewTopic(name="test", num_partitions=1, replication_factor=1),
        NewTopic(name="test2", num_partitions=1, replication_factor=1),
    ]
    admin_client.create_topics(new_topics)
    initial_topics = admin_client.list_topics()
    assert "test" in initial_topics
    assert "test2" in initial_topics

    # delete all topics by not specifying any
    kafka_fixture.delete_topics()

    final_topics = admin_client.list_topics()
    admin_client.close()
    assert "test" not in final_topics
    assert "test2" not in final_topics


@pytest.mark.asyncio
async def test_clear_topics_specific(kafka_fixture: KafkaFixture):  # noqa: F811
    """Make sure the reset function works"""
    topic_to_keep = "keep_topic"
    topic_to_clear = "clear_topic"
    partition_keep = TopicPartition(topic_to_keep, 0)
    partition_clear = TopicPartition(topic_to_clear, 0)

    await kafka_fixture.publish_event(
        payload=make_payload("clear1"), type_=TEST_TYPE, topic=topic_to_clear
    )
    await kafka_fixture.publish_event(
        payload=make_payload("keep1"), type_=TEST_TYPE, topic=topic_to_keep
    )

    consumer = KafkaConsumer(
        topic_to_keep,
        topic_to_clear,
        bootstrap_servers=kafka_fixture.kafka_servers[0],
        group_id="test",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=2000,
    )

    # Verify that both messages were consumed as expected so we can trust the consumer
    count = 0
    for record in consumer:
        count += 1
        assert record.topic in (topic_to_keep, topic_to_clear)

        # break when we get to the end, not when we reach an expected record count
        if consumer.position(partition_keep) == consumer.highwater(
            partition_keep
        ) and consumer.position(partition_clear) == consumer.highwater(partition_clear):
            break
    assert count == 2

    # Publish new messages, one to CLEAR and one to KEEP
    clear_payload2 = make_payload(topic_to_clear + "2")
    await kafka_fixture.publish_event(
        payload=clear_payload2, type_=TEST_TYPE, topic=topic_to_clear
    )

    keep_payload2 = make_payload(topic_to_keep + "2")
    await kafka_fixture.publish_event(
        payload=keep_payload2, type_=TEST_TYPE, topic=topic_to_keep
    )

    # Clear the CLEAR topic
    kafka_fixture.clear_topics(topics=topic_to_clear)

    # make sure the KEEP topic still has its event but CLEAR is empty
    records = list(consumer)
    assert len(records) == 1
    assert records[0].topic == topic_to_keep
    assert records[0].value.decode("utf-8") == json.dumps(keep_payload2)

    # Assert that we processed 1 message
    assert consumer.position(partition_clear) == consumer.highwater(partition_clear)
    assert consumer.position(partition_keep) == consumer.highwater(partition_keep)

    clear_payload3 = make_payload(topic_to_clear + "3")
    await kafka_fixture.publish_event(
        payload=clear_payload3, type_=TEST_TYPE, topic=topic_to_clear
    )

    keep_payload3 = make_payload(topic_to_keep + "3")
    await kafka_fixture.publish_event(
        payload=keep_payload3, type_=TEST_TYPE, topic=topic_to_keep
    )

    final_records = list(consumer)
    final_records.sort(key=lambda record: record.topic)
    assert len(final_records) == 2
    assert final_records[0].topic == topic_to_clear
    assert final_records[0].value.decode("utf-8") == json.dumps(clear_payload3)
    assert final_records[1].topic == topic_to_keep
    assert final_records[1].value.decode("utf-8") == json.dumps(keep_payload3)

    consumer.close()


@pytest.mark.asyncio
async def test_clear_all_topics(kafka_fixture: KafkaFixture):  # noqa: F811
    """Test clearing all topics with the kafka fixture's `clear_topics` function."""
    topic1 = "topic1"
    topic2 = "topic2"

    await kafka_fixture.publish_event(
        payload=make_payload("topic1_msg1"), type_=TEST_TYPE, topic=topic1
    )
    await kafka_fixture.publish_event(
        payload=make_payload("topic2_msg1"), type_=TEST_TYPE, topic=topic2
    )

    # clear the above two topics
    kafka_fixture.clear_topics()

    test_translator = DummyTranslator([topic1, topic2])
    async with KafkaEventSubscriber.construct(
        config=KafkaConfig(
            service_name="test_service",
            service_instance_id="1",
            kafka_servers=kafka_fixture.kafka_servers,
        ),
        translator=test_translator,
    ) as subscriber:
        # publish two new events
        await kafka_fixture.publish_event(
            payload=make_payload("topic1_msg2"), type_=TEST_TYPE, topic=topic1
        )
        await kafka_fixture.publish_event(
            payload=make_payload("topic2_msg2"), type_=TEST_TYPE, topic=topic2
        )

        # Consume the events and verify that only the new, post-deletion events are found
        await subscriber.run(False)
        await subscriber.run(False)

        topic1_events = test_translator.consumed[topic1]
        assert len(topic1_events) == 1
        assert topic1_events[0] == make_payload("topic1_msg2")

        topic2_events = test_translator.consumed[topic2]
        assert len(topic2_events) == 1
        assert topic2_events[0] == make_payload("topic2_msg2")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "events_to_publish",
    (
        [],
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "World"}, type_="test_world", key="test_key"
            ),
        ],
    ),
)
async def test_event_recorder(
    events_to_publish: Sequence[RecordedEvent],
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test event recording using the EventRecorder class."""
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    async with kafka_fixture.record_events(in_topic=topic) as recorder:
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            for event in events_to_publish:
                await event_publisher.publish(
                    payload=event.payload,
                    type_=event.type_,
                    key=event.key,
                    topic=topic,
                )

    assert recorder.recorded_events == events_to_publish


@pytest.mark.asyncio
async def test_expect_events_happy(
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test successful validation of recorded events with the expect_events method of
    the KafkaFixture.
    """
    expected_events = [
        ExpectedEvent(
            payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
        ),
        ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
    ]
    events_to_publish = [
        RecordedEvent(
            payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
        ),
        RecordedEvent(
            payload={"test_content": "World"}, type_="test_world", key="test_key"
        ),
    ]
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    async with kafka_fixture.expect_events(events=expected_events, in_topic=topic):
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            for event in events_to_publish:
                await event_publisher.publish(
                    payload=event.payload,
                    type_=event.type_,
                    key=event.key,
                    topic=topic,
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "events_to_publish",
    [
        # event payload wrong:
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "Wörld"}, type_="test_world", key="test_key"
            ),
        ],
        # event type wrong:
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "World"}, type_="test_woerld", key="test_key"
            ),
        ],
        # event key wrong:
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="wrong_key"
            ),
            RecordedEvent(
                payload={"test_content": "World"}, type_="test_world", key="wrong_key"
            ),
        ],
        # one event missing:
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
        ],
        # one event too much:
        [
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "World"}, type_="test_world", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "unexpected"},
                type_="test_unexpected",
                key="test_key",
            ),
        ],
        # wrong sequence:
        [
            RecordedEvent(
                payload={"test_content": "World"}, type_="test_world", key="test_key"
            ),
            RecordedEvent(
                payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
            ),
        ],
    ],
)
async def test_expect_events_mismatch(
    events_to_publish: Sequence[RecordedEvent],
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test the handling of mismatches between recorded and expected events using the
    expect_events method of the KafkaFixture.

    Here, we are not using the async generator interface of the EventRecorder class but
    the methods `start_recording` and `stop_and_check` so that we can nicely locate
    where the ValidationError is thrown.
    """
    expected_events = [
        ExpectedEvent(
            payload={"test_content": "Hello"}, type_="test_hello", key="test_key"
        ),
        ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
    ]
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    event_recorder = kafka_fixture.expect_events(
        events=expected_events,
        in_topic=topic,
    )
    # Usually you would use an async with block but here we are calling __aenter__ and
    # __aexit__ manually to better localize the ValidationError:
    await event_recorder.__aenter__()

    async with KafkaEventPublisher.construct(config=config) as event_publisher:
        for event in events_to_publish:
            await event_publisher.publish(
                payload=event.payload,
                type_=event.type_,
                key=event.key,
                topic=topic,
            )

    with pytest.raises(ValidationError):
        await event_recorder.__aexit__(None, None, None)

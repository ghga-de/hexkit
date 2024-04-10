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
from kafka import KafkaConsumer, TopicPartition

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    RecordedEvent,
    ValidationError,
)

pytestmark = pytest.mark.asyncio(scope="session")

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
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: Ascii
    ):
        self.consumed[topic].append(payload)


async def test_clear_topics_specific(kafka: KafkaFixture):
    """Make sure the reset function works"""
    topic_to_keep = "keep_topic"
    topic_to_clear = "clear_topic"
    partition_keep = TopicPartition(topic_to_keep, 0)
    partition_clear = TopicPartition(topic_to_clear, 0)

    await kafka.publish_event(
        payload=make_payload("clear1"), type_=TEST_TYPE, topic=topic_to_clear
    )
    await kafka.publish_event(
        payload=make_payload("keep1"), type_=TEST_TYPE, topic=topic_to_keep
    )

    consumer = KafkaConsumer(
        topic_to_keep,
        topic_to_clear,
        bootstrap_servers=kafka.kafka_servers[0],
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
    await kafka.publish_event(
        payload=clear_payload2, type_=TEST_TYPE, topic=topic_to_clear
    )

    keep_payload2 = make_payload(topic_to_keep + "2")
    await kafka.publish_event(
        payload=keep_payload2, type_=TEST_TYPE, topic=topic_to_keep
    )

    # Clear the CLEAR topic
    await kafka.clear_topics(topics=topic_to_clear)

    # make sure the KEEP topic still has its event but CLEAR is empty
    records = list(consumer)
    assert len(records) == 1
    assert records[0].topic == topic_to_keep
    assert records[0].value.decode("utf-8") == json.dumps(keep_payload2)

    # Assert that we processed 1 message
    assert consumer.position(partition_clear) == consumer.highwater(partition_clear)
    assert consumer.position(partition_keep) == consumer.highwater(partition_keep)

    clear_payload3 = make_payload(topic_to_clear + "3")
    await kafka.publish_event(
        payload=clear_payload3, type_=TEST_TYPE, topic=topic_to_clear
    )

    keep_payload3 = make_payload(topic_to_keep + "3")
    await kafka.publish_event(
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


async def test_clear_all_topics(kafka: KafkaFixture):
    """Test clearing all topics with the kafka fixture's `clear_topics` function."""
    topic1 = "topic1"
    topic2 = "topic2"

    await kafka.publish_event(
        payload=make_payload("topic1_msg1"), type_=TEST_TYPE, topic=topic1
    )
    await kafka.publish_event(
        payload=make_payload("topic2_msg1"), type_=TEST_TYPE, topic=topic2
    )

    # clear the above two topics
    await kafka.clear_topics()

    test_translator = DummyTranslator([topic1, topic2])
    async with KafkaEventSubscriber.construct(
        config=KafkaConfig(
            service_name="test_service",
            service_instance_id="1",
            kafka_servers=kafka.kafka_servers,
        ),
        translator=test_translator,
    ) as subscriber:
        # publish two new events
        await kafka.publish_event(
            payload=make_payload("topic1_msg2"), type_=TEST_TYPE, topic=topic1
        )
        await kafka.publish_event(
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
    events_to_publish: Sequence[RecordedEvent], kafka: KafkaFixture
):
    """Test event recording using the EventRecorder class."""
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )

    async with kafka.record_events(in_topic=topic) as recorder:
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            for event in events_to_publish:
                await event_publisher.publish(
                    payload=event.payload,
                    type_=event.type_,
                    key=event.key,
                    topic=topic,
                )

    assert recorder.recorded_events == events_to_publish


async def test_clear_topics_with_recorder(kafka: KafkaFixture):
    """Test the behavior of the event recorder is not affected by clear_topics().

    For context: `clear_topics()` does not wipe out topic offsets. That has no impact on
    normal event consumption in testing (e.g. `subscriber.run(forever=False)`), but
    `EventRecorder` retrieves the offsets upon both start and stop, then calculates the
    difference as the number of events it should listen to. If an EventRecorder has been
    used and persists (like in session-scoped testing), then a gap can arise between its
    `__starting_offsets` and `latest_offsets`.
    """
    topic: Ascii = "test_topic"
    payload: JsonObject = {"test_content": "Hello"}
    type_: Ascii = "test_type"
    key: Ascii = "test_key"

    # Trigger creation of the consumer and consume an event to establish consumer offsets.
    async with kafka.record_events(in_topic=topic):
        await kafka.publish_event(payload=payload, topic=topic, type_=type_, key=key)

    # Publish some events that don't get consumed in order to push out the
    # topic's log end offset
    for _ in range(5):
        await kafka.publish_event(payload=payload, topic=topic, type_=type_, key=key)

    # Clear all records from the topic (leaving offsets intact)
    await kafka.clear_topics()

    # Use the event recorder again. The consumer should skip to the end of the topic
    # before yielding control, thus avoiding the problem of trying to consume events
    # that don't exist. pre-fix would have expected 6 events rather than 1.
    async with kafka.record_events(in_topic=topic):
        await kafka.publish_event(payload=payload, topic=topic, type_=type_, key=key)


async def test_expect_events_happy(kafka: KafkaFixture):
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
        kafka_servers=kafka.kafka_servers,
    )

    async with kafka.expect_events(events=expected_events, in_topic=topic):
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            for event in events_to_publish:
                await event_publisher.publish(
                    payload=event.payload,
                    type_=event.type_,
                    key=event.key,
                    topic=topic,
                )


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
    events_to_publish: Sequence[RecordedEvent], kafka: KafkaFixture
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
        kafka_servers=kafka.kafka_servers,
    )

    event_recorder = kafka.expect_events(
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

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

"""Testing of the Kafka testutils."""

import asyncio
import json
import logging
from collections.abc import Sequence
from contextlib import nullcontext
from typing import Optional

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from hexkit.correlation import set_correlation_id
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
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)

log = logging.getLogger("tester")
log.setLevel("ERROR")
pytestmark = pytest.mark.asyncio()


TEST_TYPE = "test_type"
DEFAULT_CORRELATION_ID = "513ed283-478e-428e-8c2f-ff6da36a9527"
OTHER_CORRELATION_ID = "d4fd8051-293b-4cce-9970-6b78ca149bc0"


def make_payload(msg: str) -> JsonObject:
    """Return a test payload with a specified string."""
    return {"test_msg": msg}


class DummyTranslator(EventSubscriberProtocol):
    """An event subscriber translator that simply tracks what it consumes."""

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
    """Make sure the reset function works."""
    partition_keep = TopicPartition("keep_topic", 0)
    partition_clear = TopicPartition("clear_topic", 0)

    for topic in "keep_topic", "clear_topic":
        await kafka.publish_event(
            payload=make_payload(f"payload 1 for {topic}"),
            type_=TEST_TYPE,
            topic=topic,
        )

    consumer = AIOKafkaConsumer(
        "clear_topic",
        "keep_topic",
        bootstrap_servers=kafka.kafka_servers[0],
        group_id="test",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=2000,
    )
    await consumer.start()
    try:
        # Verify that both messages were consumed as expected so we can trust the consumer
        count = 0
        async for record in consumer:
            count += 1
            assert record.topic in ("clear_topic", "keep_topic")

            # break when we get to the end, not when we reach an expected record count
            if await consumer.position(partition_keep) == consumer.highwater(
                partition_keep
            ) and await consumer.position(partition_clear) == consumer.highwater(
                partition_clear
            ):
                break
        assert count == 2
        log.error("Confirmed that we got the first two events")
        await consumer.commit()

        # Publish new messages to both topics
        for topic in "clear_topic", "keep_topic":
            await kafka.publish_event(
                payload=make_payload(f"payload 2 for {topic}"),
                type_=TEST_TYPE,
                topic=topic,
            )
        await asyncio.sleep(1)
        log.error("Published the 2nd round of messages")

        # Clear the clear_topic
        # await kafka.clear_topics(topics="clear_topic")
        # log.error("Topics have been cleared (supposedly)")

        # make sure the keep_topic still has its event but clear_topic is empty
        prefetched = await consumer.getmany(timeout_ms=500)
        assert len(prefetched) == 1
        records = next(iter(prefetched.values()))
        assert len(records) == 1
        assert records[0].topic == "keep_topic"
        assert records[0].value
        assert records[0].value.decode("utf-8") == json.dumps(
            make_payload("payload 2 for keep_topic")
        )

        log.error("Confirmed that 'clear_topic' was cleared!")

        # Publish more messages to both topics
        for topic in "clear_topic", "keep_topic":
            await kafka.publish_event(
                payload=make_payload(f"payload 3 for {topic}"),
                type_=TEST_TYPE,
                topic=topic,
            )
        await asyncio.sleep(1)

        log.error("Published the 3rd round of messages")

        # make sure messages are consumed again
        records = []
        while prefetched := await consumer.getmany(timeout_ms=500):
            for records_for_topic in prefetched.values():
                records.extend(records_for_topic)

        log.error("Fetched the messages, now examining.")
        await consumer.commit()

        print("\n".join([str(_) for _ in records]))
        assert len(records) == 2
        records.sort(key=lambda record: record.topic)
        assert records[0].topic == "clear_topic"
        assert records[0].value
        assert records[0].value.decode("utf-8") == json.dumps(
            make_payload("payload 3 for clear_topic")
        )
        assert records[1].topic == "keep_topic"
        assert records[1].value
        assert records[1].value.decode("utf-8") == json.dumps(
            make_payload("payload 3 for keep_topic")
        )
    finally:
        await consumer.stop()


async def test_clear_all_topics(kafka: KafkaFixture):
    """Test clearing all topics with the kafka fixture's `clear_topics` function."""
    topics = ["topic1", "topic2"]

    for topic in topics:
        await kafka.publish_event(
            payload=make_payload(f"msg 1 for {topic}"), type_=TEST_TYPE, topic=topic
        )

    # clear the above two topics
    await kafka.clear_topics()

    # publish two new events
    for topic in topics:
        await kafka.publish_event(
            payload=make_payload(f"msg 2 for {topic}"), type_=TEST_TYPE, topic=topic
        )

    test_translator = DummyTranslator(topics)
    async with KafkaEventSubscriber.construct(
        config=KafkaConfig(
            service_name="test_service",
            service_instance_id="1",
            kafka_servers=kafka.kafka_servers,
        ),
        translator=test_translator,
    ) as subscriber:
        # Consume the events and verify that only the new, post-deletion events are found
        await subscriber.run(False)
        await subscriber.run(False)

        for topic in topics:
            events = test_translator.consumed[topic]
            assert len(events) == 1
            assert events[0] == make_payload(f"msg 2 for {topic}")


@pytest.mark.parametrize(
    "events_to_publish,capture_headers",
    (
        ([], False),
        ([], True),
        (
            [
                RecordedEvent(
                    payload={"test_content": "Hello"},
                    type_="test_hello",
                    key="test_key",
                ),
                RecordedEvent(
                    payload={"test_content": "World"},
                    type_="test_world",
                    key="test_key",
                ),
            ],
            False,
        ),
        (
            [
                RecordedEvent(
                    payload={"test_content": "Hello"},
                    type_="test_hello",
                    key="test_key",
                    headers={"correlation_id": DEFAULT_CORRELATION_ID},
                ),
                RecordedEvent(
                    payload={"test_content": "World"},
                    type_="test_world",
                    key="test_key",
                    headers={"correlation_id": DEFAULT_CORRELATION_ID},
                ),
            ],
            True,
        ),
    ),
)
async def test_event_recorder(
    events_to_publish: Sequence[RecordedEvent],
    capture_headers: bool,
    kafka: KafkaFixture,
):
    """Test event recording using the EventRecorder class."""
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )

    async with (
        set_correlation_id(DEFAULT_CORRELATION_ID),
        kafka.record_events(
            in_topic=topic, capture_headers=capture_headers
        ) as recorder,
        KafkaEventPublisher.construct(config=config) as event_publisher,
    ):
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
            payload={"test_content": "Hello"},
            type_="test_hello",
            key="test_key",
        ),
        RecordedEvent(
            payload={"test_content": "World"},
            type_="test_world",
            key="test_key",
        ),
    ]
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )

    async with (
        kafka.expect_events(events=expected_events, in_topic=topic),
        KafkaEventPublisher.construct(config=config) as event_publisher,
    ):
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


@pytest.mark.parametrize(
    "recorded_correlation_id", [DEFAULT_CORRELATION_ID, OTHER_CORRELATION_ID]
)
@pytest.mark.parametrize(
    "expected_headers",
    [
        None,
        {"correlation_id": OTHER_CORRELATION_ID},
        {"correlation_id": DEFAULT_CORRELATION_ID},
    ],
)
@pytest.mark.parametrize("capture_headers", [True, False])
async def test_capture_headers(
    expected_headers: Optional[dict[str, str]],
    recorded_correlation_id: str,
    capture_headers: bool,
    kafka: KafkaFixture,
):
    """Test the handling of mismatches between recorded and expected events using the
    expect_events method of the KafkaFixture, but specifically with the optional headers.
    For now, the only header to be tested is the correlation_id.

    The expected behavior is that the headers are compared when provided as expected
    arguments, otherwise ignored.
    """
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )
    expected_event = ExpectedEvent(
        payload={"test_content": "Hello"},
        type_="test_type",
        headers=expected_headers,
    )

    # Set up the event recorder with the expected event and the capture_headers flag
    event_recorder = kafka.expect_events(
        events=[expected_event], in_topic=topic, capture_headers=capture_headers
    )

    # start the recorder
    await event_recorder.__aenter__()

    # Set the correlation ID context var and publish the 'recorded' event
    async with (
        set_correlation_id(recorded_correlation_id),
        KafkaEventPublisher.construct(config=config) as event_publisher,
    ):
        await event_publisher.publish(
            payload=expected_event.payload,
            type_=expected_event.type_,
            key="",
            topic=topic,
        )

    # only time there should be an error is when 1. expected headers are provided
    # and 2. capture headers flag isn't set or expected/recorded headers don't match
    expected_error = expected_headers and (
        not capture_headers
        or expected_headers.get("correlation_id", "") != recorded_correlation_id
    )
    with pytest.raises(ValidationError) if expected_error else nullcontext():
        await event_recorder.__aexit__(None, None, None)


async def test_clear_compacted_topics(kafka: KafkaFixture):
    """Verify that the Kafka test fixture can clear compacted topics without
    encountering a policy violation error.
    """
    topic = "mytopic"
    await kafka.publish_event(
        payload=make_payload(f"msg 1 for {topic}"), type_=TEST_TYPE, topic=topic
    )
    await kafka.set_cleanup_policy(topic=topic, policy="compact")
    assert await kafka.get_cleanup_policy(topic=topic) == "compact"

    await kafka.clear_topics(topics=topic)

    # Verify that the cleanup policy was set back to 'compact'
    assert await kafka.get_cleanup_policy(topic=topic) == "compact"

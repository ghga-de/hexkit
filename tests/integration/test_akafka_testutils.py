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

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
)
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    RecordedEvent,
    ValidationError,
    kafka_fixture,  # noqa: F401
)

TEST_TYPE = "test_type"
KEEP = "keep_topic"
CLEAR = "clear_topic"
KEEP_TOPIC_PARTITION = TopicPartition(KEEP, 0)
CLEAR_TOPIC_PARTITION = TopicPartition(CLEAR, 0)


def make_payload(msg: str) -> JsonObject:
    """Return a test payload with a specified string."""
    return {"test_msg": msg}


@pytest.mark.asyncio
async def test_clear_topics_specific(kafka_fixture: KafkaFixture):  # noqa: F811
    """Make sure the reset function works"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_fixture.config.kafka_servers
    )

    await kafka_fixture.publish_event(
        payload=make_payload("clear1"), type_=TEST_TYPE, topic=CLEAR
    )
    await kafka_fixture.publish_event(
        payload=make_payload("keep1"), type_=TEST_TYPE, topic=KEEP
    )

    assert CLEAR in admin_client.list_topics()
    assert KEEP in admin_client.list_topics()

    consumer = KafkaConsumer(
        KEEP,
        CLEAR,
        bootstrap_servers=kafka_fixture.kafka_servers[0],
        group_id="test",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    # Verify that both messages were consumed as expected so we can trust the consumer
    count = 0
    for record in consumer:
        count += 1
        assert record.topic in (KEEP, CLEAR)

        # break when we get to the end, not when we reach an expected record count
        if consumer.position(KEEP_TOPIC_PARTITION) == consumer.highwater(
            KEEP_TOPIC_PARTITION
        ) and consumer.position(CLEAR_TOPIC_PARTITION) == consumer.highwater(
            CLEAR_TOPIC_PARTITION
        ):
            break
    assert count == 2

    # Publish new messages, one to CLEAR and one to KEEP
    clear_payload2 = make_payload(CLEAR + "2")
    await kafka_fixture.publish_event(
        payload=clear_payload2, type_=TEST_TYPE, topic=CLEAR
    )

    keep_payload2 = make_payload(KEEP + "2")
    await kafka_fixture.publish_event(
        payload=keep_payload2, type_=TEST_TYPE, topic=KEEP
    )

    # Clear the CLEAR topic
    kafka_fixture.clear_topics(CLEAR)

    # Set a reasonable timeout to check all messages but not run indefinitely
    consumer.config["consumer_timeout_ms"] = 2000

    # make sure the KEEP topic still has its event but CLEAR is empty
    total = 0
    for record in consumer:
        total += 1
        assert record.topic == KEEP
        assert record.value.decode("utf-8") == json.dumps(keep_payload2)
    # Assert that we processed 1 message
    assert total == 1
    assert consumer.position(CLEAR_TOPIC_PARTITION) == consumer.highwater(
        CLEAR_TOPIC_PARTITION
    )
    assert consumer.position(KEEP_TOPIC_PARTITION) == consumer.highwater(
        KEEP_TOPIC_PARTITION
    )

    clear_payload3 = make_payload(CLEAR + "3")
    await kafka_fixture.publish_event(
        payload=clear_payload3, type_=TEST_TYPE, topic=CLEAR
    )

    keep_payload3 = make_payload(KEEP + "3")
    await kafka_fixture.publish_event(
        payload=keep_payload3, type_=TEST_TYPE, topic=KEEP
    )

    total = 0
    keep = 0
    clear = 0
    for record in consumer:
        total += 1
        if record.topic == KEEP:
            keep += 1
            assert record.value.decode("utf-8") == json.dumps(keep_payload3)
        elif record.topic == CLEAR:
            clear += 1
            assert record.value.decode("utf-8") == json.dumps(clear_payload3)

    assert total == 2
    assert keep == 1
    assert clear == 1

    consumer.close()


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

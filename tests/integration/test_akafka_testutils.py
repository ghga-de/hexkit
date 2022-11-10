# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

from typing import Sequence

import pytest

from hexkit.providers.akafka import KafkaConfig, KafkaEventPublisher
from hexkit.providers.akafka.testutils import kafka_fixture  # noqa: F401
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    RecordedEvent,
    ValidationError,
)


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
async def test_expect_events_missmatch(
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

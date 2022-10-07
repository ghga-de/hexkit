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

"""Testing of the Kafka testutils. Only includes tests that are not already covered by
the `test_kafka` module."""

from typing import Sequence

import pytest

from hexkit.providers.akafka import KafkaConfig, KafkaEventPublisher
from hexkit.providers.akafka.testutils import kafka_fixture  # noqa: F401
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    ValidationError,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "events_to_publish, key_to_publish",
    [
        # event payload wrong:
        (
            [
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
                ExpectedEvent(payload={"test_content": "Wörld"}, type_="test_world"),
            ],
            "test_key",
        ),
        # event type wrong:
        (
            [
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
                ExpectedEvent(payload={"test_content": "World"}, type_="test_woerld"),
            ],
            "test_key",
        ),
        # event key wrong:
        (
            [
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
                ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
            ],
            "wrong_key",
        ),
        # one event missing:
        (
            [
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
            ],
            "test_key",
        ),
        # one event too much:
        (
            [
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
                ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
                ExpectedEvent(
                    payload={"test_content": "unexpected"}, type_="test_unexpected"
                ),
            ],
            "test_key",
        ),
        # wrong sequence:
        (
            [
                ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
                ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
            ],
            "test_key",
        ),
    ],
)
async def test_event_recorder_missmatch(
    events_to_publish: Sequence[ExpectedEvent],
    key_to_publish: str,
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test the handling of mismatches between recorded and expected events.

    Here, we are not using the async generator interface of the EventRecorder class but
    the methods `start_recording` and `stop_and_check` so that we can nicely locate
    where the ValidationError is thrown.
    """

    expected_events = [
        ExpectedEvent(payload={"test_content": "Hello"}, type_="test_hello"),
        ExpectedEvent(payload={"test_content": "World"}, type_="test_world"),
    ]
    expected_key = "test_key"
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    event_recorder = kafka_fixture.expect_events(
        events=expected_events,
        in_topic=topic,
        with_key=expected_key,
    )
    await event_recorder.start_recording()

    async with KafkaEventPublisher.construct(config=config) as event_publisher:
        for event in events_to_publish:
            await event_publisher.publish(
                payload=event.payload,
                type_=event.type_,
                key=key_to_publish,
                topic=topic,
            )

    with pytest.raises(ValidationError):
        await event_recorder.stop_and_check()

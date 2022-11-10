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

"""Testing Apache Kafka based providers."""

from unittest.mock import AsyncMock

import pytest

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testutils import (  # noqa: F401
    ExpectedEvent,
    KafkaFixture,
    kafka_fixture,
)


@pytest.mark.asyncio
async def test_kafka_event_publisher(kafka_fixture: KafkaFixture):  # noqa: F811
    """Test the KafkaEventPublisher."""
    payload: JsonObject = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    async with kafka_fixture.expect_events(
        events=[ExpectedEvent(payload=payload, type_=type_, key=key)],
        in_topic=topic,
    ):
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            await event_publisher.publish(
                payload=payload,
                type_=type_,
                key=key,
                topic=topic,
            )


@pytest.mark.asyncio
async def test_kafka_event_subscriber(kafka_fixture: KafkaFixture):  # noqa: F811
    """Test the KafkaEventSubscriber with mocked KafkaEventSubscriber."""
    payload = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    # create protocol-compatiple translator mock:
    translator = AsyncMock()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    # publish one event:
    await kafka_fixture.publish_event(
        topic=topic, payload=payload, key=key, type_=type_
    )

    # setup the provider:
    config = KafkaConfig(
        service_name="event_subscriber",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )
    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
    ) as event_subscriber:
        # consume one event:
        await event_subscriber.run(forever=False)

    # check if the translator was called correctly:
    translator.consume.assert_awaited_once_with(
        payload=payload, type_=type_, topic=topic
    )

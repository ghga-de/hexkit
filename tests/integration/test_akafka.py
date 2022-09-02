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

import json
from unittest.mock import AsyncMock

import pytest
from kafka import KafkaConsumer, KafkaProducer
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from tests.fixtures.utils import exec_with_timeout


@pytest.mark.asyncio
async def test_kafka_event_publisher():
    """Test the KafkaEventPublisher."""
    payload: JsonObject = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    with KafkaContainer() as kafka:
        bootstrap_servers = [kafka.get_bootstrap_server()]
        config = KafkaConfig(
            service_name="test_publisher",
            service_instance_id="1",
            kafka_servers=bootstrap_servers,
        )

        async with KafkaEventPublisher.construct(config=config) as event_publisher:

            await event_publisher.publish(
                payload=payload,
                type_=type_,
                key=key,
                topic=topic,
            )

        # consume event using the python-kafka library directly:
        consumer = KafkaConsumer(
            topic,
            client_id="test_consumer",
            group_id="test_consumer_group",
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            key_deserializer=lambda key: key.decode("ascii"),
            value_deserializer=lambda val: json.loads(val.decode("ascii")),
        )
        try:
            received_event = exec_with_timeout(lambda: next(consumer), timeout_after=4)
        finally:
            consumer.close()

        # check if received event matches the expectations:
        assert payload == received_event.value
        assert received_event.headers[0][0] == "type"
        received_header_dict = {
            header[0]: header[1].decode("ascii") for header in received_event.headers
        }
        assert type_ == received_header_dict["type"]
        assert key == received_event.key


@pytest.mark.asyncio
async def test_kafka_event_subscriber():
    """Test the KafkaEventSubscriber with mocked KafkaEventSubscriber."""
    payload = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"
    headers = [("type", b"test_type")]

    # create protocol-compatiple translator mock:
    translator = AsyncMock()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    with KafkaContainer() as kafka:
        # publish one event the python-kafka library directly:
        bootstrap_servers = [kafka.get_bootstrap_server()]

        producer = KafkaProducer(
            client_id="test_producer",
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda key: key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )
        try:
            producer.send(topic=topic, value=payload, key=key, headers=headers)
        finally:
            producer.close()

        # setup the provider:
        config = KafkaConfig(
            service_name="event_subscriber",
            service_instance_id="1",
            kafka_servers=bootstrap_servers,
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

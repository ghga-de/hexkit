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

from collections import namedtuple
from contextlib import nullcontext
from typing import Optional
from unittest.mock import AsyncMock, Mock

import pytest

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)


@pytest.mark.asyncio
async def test_kafka_event_publisher():
    """Test the KafkaEventPublisher with mocked KafkaEventPublisher."""
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"
    payload = {"test_content": "Hello World"}
    expected_headers = [("type", b"test_type")]

    # create kafka producer mock
    producer = AsyncMock()
    producer_class = Mock(return_value=producer)

    # publish event using the provider:
    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=["my-fake-kafka-server"],
    )
    async with KafkaEventPublisher.construct(
        config=config,
        kafka_producer_cls=producer_class,
    ) as event_publisher:

        # check if producer class was called correctly:
        producer_class.assert_called_once()
        pc_kwargs = producer_class.call_args.kwargs
        assert pc_kwargs["client_id"] == "test_publisher.1"
        assert pc_kwargs["bootstrap_servers"] == ["my-fake-kafka-server"]
        assert callable(pc_kwargs["key_serializer"])
        assert callable(pc_kwargs["value_serializer"])
        producer.start.assert_awaited_once()

        # publish one event:
        await event_publisher.publish(
            payload=payload,
            type_=type_,
            key=key,
            topic=topic,
        )

    # check if producer was correctly used:
    producer.send_and_wait.assert_awaited_once_with(
        topic,
        value=payload,
        key=key,
        headers=expected_headers,
    )
    producer.stop.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "type_, headers, is_translator_called, processing_failure, exception",
    [
        (
            "test_type",
            [("type", b"test_type")],
            True,
            False,
            None,
        ),
        (
            "uninteresting_type",
            [("type", b"uninteresting_type")],
            False,
            False,
            None,
        ),
        (
            "test_type",
            [],  # type header missing => event should be ignored
            False,
            False,
            None,
        ),
        (
            "test_type",
            [("type", b"test_type")],
            True,
            True,  # Simulate processing failure
            RuntimeError,
        ),
    ],
)
async def test_kafka_event_subscriber(
    type_: str,
    headers: list[tuple[str, bytes]],
    is_translator_called: bool,
    processing_failure: bool,
    exception: Optional[type[Exception]],
):
    """Test the KafkaEventSubscriber with mocked KafkaEventSubscriber."""
    service_name = "event_subscriber"
    topic = "test_topic"
    types_of_interest = ["test_type"]
    payload: JsonObject = {"test": "Hello World!"}

    # mock event:
    Event = namedtuple(
        "Event",
        ["topic", "key", "value", "headers", "partition", "offset"],
    )
    event = Event(
        key="test_key",
        headers=headers,
        value=payload,
        topic=topic,
        partition=1,
        offset=0,
    )

    # create kafka consumer mock:
    consumer = AsyncMock()
    consumer.__anext__.return_value = event
    consumer_cls = Mock()
    consumer_cls.return_value = consumer

    # create protocol-compatiple translator mock:
    translator = AsyncMock()
    if processing_failure and exception:
        translator.consume.side_effect = exception()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = types_of_interest

    # setup the provider:
    config = KafkaConfig(
        service_name=service_name,
        service_instance_id="1",
        kafka_servers=["my-fake-kafka-server"],
    )
    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
        kafka_consumer_cls=consumer_cls,
    ) as event_subscriber:

        # check if consumer class was called correctly:
        consumer_cls.assert_called_once()
        cc_kwargs = consumer_cls.call_args.kwargs
        assert cc_kwargs["client_id"] == "event_subscriber.1"
        assert cc_kwargs["bootstrap_servers"] == ["my-fake-kafka-server"]
        assert cc_kwargs["group_id"] == service_name
        assert cc_kwargs["auto_offset_reset"] == "earliest"
        assert callable(cc_kwargs["key_deserializer"])
        assert callable(cc_kwargs["value_deserializer"])

        # consume one event:
        with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
            await event_subscriber.run(forever=False)

    # check if producer was correctly started and stopped:
    consumer.start.assert_awaited_once()
    consumer.stop.assert_awaited_once()

    # check if the translator was called correctly:
    if is_translator_called:
        translator.consume.assert_awaited_once_with(
            payload=payload, type_=type_, topic=topic
        )
    else:
        assert translator.consume.await_count == 0

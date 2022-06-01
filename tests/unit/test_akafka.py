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

from contextlib import nullcontext
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber


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
    async with KafkaEventPublisher.construct(
        service_name="test_publisher",
        client_suffix="1",
        kafka_servers=["my-fake-kafka-server"],
        kafka_producer_cls=producer_class,
    ) as event_publisher:

        # check if producer class was called correctly:
        producer_class.assert_called_once()
        pc_kwargs = producer_class.call_args.kwargs
        assert pc_kwargs["client_id"] == "test_publisher.1"
        assert pc_kwargs["bootstrap_servers"] == ["my-fake-kafka-server"]
        assert callable(pc_kwargs["key_serializer"])
        assert callable(pc_kwargs["value_serializer"])

        # publish one event:
        await event_publisher.publish(
            payload=payload,
            type_=type_,
            key=key,
            topic=topic,
        )

    # check if producer was correctly used:
    producer.start.assert_awaited_once()
    producer.send_and_wait.assert_awaited_once_with(
        topic,
        value=payload,
        key=key,
        headers=expected_headers,
    )
    producer.stop.assert_awaited_once()


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
def test_kafka_event_subscriber(
    type_: str,
    headers: list[tuple[str, bytes]],
    is_translator_called: bool,
    processing_failure: bool,
    exception: Optional[type[Exception]],
):
    """Test the KafkaEventSubscriber with mocked KafkaEventSubscriber."""
    topic = "test_topic"
    types_of_interest = ["test_type"]
    payload: JsonObject = {"test": "Hello World!"}

    # mock event:
    event = Mock()
    event.key = "test_key"
    event.headers = headers
    event.value = payload
    event.topic = topic

    # create kafka consumer mock:
    consumer = MagicMock()
    consumer.__next__.return_value = event
    consumer_cls = Mock()
    consumer_cls.return_value = consumer

    # create protocol-compatiple translator mock:
    translator = Mock()
    if processing_failure and exception:
        translator.consume.side_effect = exception()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = types_of_interest

    # setup the provider:
    as_resource = KafkaEventSubscriber.as_resource(
        service_name="event_subscriber",
        client_suffix="1",
        kafka_servers=["my-fake-kafka-server"],
        translator=translator,
        kafka_consumer_cls=consumer_cls,
    )

    event_subscriber = next(as_resource)
    try:
        # consume one event:
        with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
            event_subscriber.run(forever=False)
    finally:
        with pytest.raises(StopIteration):
            next(as_resource)

    # check if consumer was closed correctly:
    consumer.close.assert_called_once()

    # check if the translator was called correctly:
    if is_translator_called:
        translator.consume.assert_called_once_with(
            payload=payload, type_=type_, topic=topic
        )
    else:
        assert translator.consume.call_count == 0

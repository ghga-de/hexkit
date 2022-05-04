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
from unittest.mock import Mock

import pytest
from kafka import KafkaConsumer, KafkaProducer
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from tests.fixtures.utils import exec_with_timeout


def test_kafka_event_publisher():
    """Test the KafkaEventPublisher."""
    payload: JsonObject = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    with KafkaContainer() as kafka:

        as_resource = KafkaEventPublisher.as_resource(
            service_name="test_publisher",
            client_suffix="1",
            kafka_servers=[kafka.get_bootstrap_server()],
        )

        # publish event using the provider:
        event_publisher = next(as_resource)
        try:
            event_publisher.publish(
                payload=payload,
                type_=type_,
                key=key,
                topic=topic,
            )
        finally:
            with pytest.raises(StopIteration):
                next(as_resource)

        # consume event using the python-kafka library directly:
        consumer = KafkaConsumer(
            topic,
            client_id="test_consumer",
            group_id="test_consumer_group",
            bootstrap_servers=[kafka.get_bootstrap_server()],
            auto_offset_reset="earliest",
            key_deserializer=lambda key: key.decode("ascii"),
            value_deserializer=lambda val: json.loads(val.decode("ascii")),
        )
        try:
            received_event = exec_with_timeout(lambda: next(consumer), timeout_after=2)
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


def test_kafka_event_subscriber():
    """Test the KafkaEventSubscriber with mocked KafkaEventSubscriber."""
    payload = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"
    headers = [("type", b"test_type")]

    # create protocol-compatiple translator mock:
    translator = Mock()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    with KafkaContainer() as kafka:
        # publish one event the python-kafka library directly:
        producer = KafkaProducer(
            client_id="test_producer",
            bootstrap_servers=[kafka.get_bootstrap_server()],
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
        as_resource = KafkaEventSubscriber.as_resource(
            service_name="event_subscriber",
            client_suffix="1",
            kafka_servers=[kafka.get_bootstrap_server()],
            translator=translator,
        )

        event_subscriber = next(as_resource)
        try:
            # consume one event:
            exec_with_timeout(
                lambda: event_subscriber.run(forever=False), timeout_after=2
            )
        finally:
            with pytest.raises(StopIteration):
                next(as_resource)

        # check if the translator was called correctly:
        translator.consume.assert_called_once_with(
            payload=payload, type_=type_, topic=topic
        )

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

from unittest.mock import Mock

import pytest
from black import nullcontext

from hexkit.providers.akafka import KafkaEventPublisher, NonAsciiStrError


@pytest.mark.parametrize(
    "type_, key_, topic, expected_headers, exception",
    [
        ("test_type", "test_key", "test_topic", [("type", b"test_type")], None),
        (
            "test_ßtype",  # non ascii
            "test_key",
            "test_topic",
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_ßkey",  # non ascii
            "test_topic",
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_key",
            "test_ßtopic",  # non ascii
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
    ],
)
def test_kafka_event_publisher(type_, key_, topic, expected_headers, exception):
    """Test the KafkaEventPublisher with mocked KafkaEventPublisher."""
    payload = {"test_content": "Hello World"}

    # create kafka producer mock
    producer_class = Mock()

    # publish event using the provider:
    event_publisher = KafkaEventPublisher(
        service_name="test_publisher",
        client_suffix="1",
        kafka_servers=["my-fake-kafka-server"],
        kafka_producer_class=producer_class,
    )

    # check if producer class was called correctly:
    producer_class.assert_called_once()
    pc_kwargs = producer_class.call_args.kwargs
    assert pc_kwargs["client_id"] == "test_publisher.1"
    assert pc_kwargs["bootstrap_servers"] == ["my-fake-kafka-server"]
    assert callable(pc_kwargs["key_serializer"])
    assert callable(pc_kwargs["value_serializer"])

    with (pytest.raises(exception) if exception else nullcontext()):
        event_publisher.publish(
            payload=payload,
            type_=type_,
            key_=key_,
            topic=topic,
        )

    if not exception:
        # check if producer was correctly used:
        producer = producer_class.return_value
        producer.send.assert_called_once_with(
            topic,
            value=payload,
            key=key_,
            headers=expected_headers,
        )
        producer.flush.assert_called_once()

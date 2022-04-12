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

from kafka import KafkaConsumer
from testcontainers.kafka import KafkaContainer

from hexkit.eventpub.providers.akafka import KafkaEventPublisher
from hexkit.utils import exec_with_timeout


def test_kafka_event_publisher():
    """Test the KafkaEventPublisher."""
    event_payload = {"test_content": "Hello World"}
    event_type = "test_event"
    event_key = "test_key"
    topic = "test_topic"

    with KafkaContainer() as kafka:

        # publish event using the provider:
        event_publisher = KafkaEventPublisher(
            service_name="test_publisher",
            client_suffix="1",
            kafka_servers=[kafka.get_bootstrap_server()],
        )

        event_publisher.publish(
            event_payload=event_payload,
            event_type=event_type,
            event_key=event_key,
            topic=topic,
        )

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

        received_event = exec_with_timeout(lambda: next(consumer), timeout_after=2)

        # check if received event matches the expectations:
        assert event_payload == received_event.value
        assert received_event.headers[0][0] == "type"
        received_header_dict = {
            header[0]: header[1].decode("ascii") for header in received_event.headers
        }
        assert event_type == received_header_dict["type"]
        assert event_key == received_event.key

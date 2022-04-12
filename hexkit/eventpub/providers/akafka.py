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

"""
Apache Kafka-specific event publishing provider.

Require dependencies of the `akafka` extra. See the `setup.cfg`.
"""

import json

from kafka import KafkaProducer

from hexkit.custom_types import JSON
from hexkit.eventpub.protocol import EventPublisherProtocol


class NonAsciiStrError(RuntimeError):
    """Thrown when non-ASCII string was unexpectedly provided"""

    pass  # pylint: disable=unnecessary-pass


class KafkaEventPublisher(EventPublisherProtocol):
    """Apache Kafka specific event publishing provider."""

    def __init__(
        self,
        *,
        service_name: str,
        client_suffix: str,
        kafka_servers: list[str],
        kafka_producer_class=KafkaProducer,
    ):
        """Initialize the provider with some config params.

        Args:
            service_name (str):
                The name of the (micro-)service from which messages are published.
            client_suffix (str):
                String that uniquely identifies this instance across all instances of this
                service. Will create a globally unique Kafka client ID by concatenating
            kafka_servers (list[str]):
                List of connection strings pointing to the kafka brokers.
            kafka_producer_class:
                Overwrite the used Kafka Producer class. Only intented for unit testing.
        """

        client_id = f"{service_name}.{client_suffix}"

        self._producer = kafka_producer_class(
            bootstrap_servers=kafka_servers,
            client_id=client_id,
            key_serializer=lambda event_key: event_key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

    def publish(
        self, *, event_payload: JSON, event_type: str, event_key: str, topic: str
    ) -> None:
        """Publish an event to an Apache Kafka event broker.

        Args:
            event_payload (JSON): The payload to ship with the event.
            event_type (str): The event type. ASCII characters only.
            event_key (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        if not (event_type.isascii() and event_key.isascii() and topic.isascii()):
            raise NonAsciiStrError(
                "event_type, event_key, and topic_name should be ascii only."
            )

        event_headers = [("type", event_type.encode("ascii"))]
        self._producer.send(
            topic, key=event_key, value=event_payload, headers=event_headers
        )
        self._producer.flush()

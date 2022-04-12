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
Apache Kafka specific event publishing provider.

Require dependencies of the `akafka` extra. See the `setup.cfg`.
"""

import json

from kafka import KafkaProducer

from hexkit.custom_types import JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol


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

        self.client_id = f"{service_name}.{client_suffix}"

        self._producer = kafka_producer_class(
            bootstrap_servers=kafka_servers,
            client_id=self.client_id,
            key_serializer=lambda key_: key_.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

    def publish(
        self, *, payload: JsonObject, type_: str, key_: str, topic: str
    ) -> None:
        """Publish an event to an Apache Kafka event broker.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key_ (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        if not (type_.isascii() and key_.isascii() and topic.isascii()):
            raise NonAsciiStrError("type_, key_, and topic should be ascii only.")

        event_headers = [("type", type_.encode("ascii"))]
        self._producer.send(topic, key=key_, value=payload, headers=event_headers)
        self._producer.flush()

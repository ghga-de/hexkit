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

from hexkit.custom_types import JSON
from hexkit.eventpub.protocol import EventPublisherProto


class KafkaEventPublisher(EventPublisherProto):
    """Apache Kafka specific event publishing provider."""

    def __init__(self, service_name: str, client_suffix: str, kafka_servers: list[str]):
        """Initialize the provider with some config params.

        Args:
            service_name (str):
                The name of the (micro-)service from which messages are published.
            client_suffix (str):
                String that uniquely this instance across all instances of this service.
                Will create a globally unique Kafka client IDidentifier by concatenating
                the service_name and the client_suffix.
            kafka_servers (list[str]):
                List of connection strings pointing to the kafka brokers.
        """

        self.client_id = f"{service_name}.{client_suffix}"

        self._producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            client_id=self.client_id,
            key_serializer=lambda event_key: event_key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

    def publish(
        self, *, event_payload: JSON, event_type: str, event_key: str, topic_name: str
    ) -> None:
        """Publish event to Kafka broker."""

        event_value = {
            "type": event_type,
            "payload": event_payload,
        }
        self._producer.send(topic_name, key=event_key, value=event_value)
        self._producer.flush()

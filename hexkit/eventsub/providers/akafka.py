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
Apache Kafka-specific event subscription provider.

Require dependencies of the `akafka` extra. See the `setup.cfg`.
"""

import json

from kafka import KafkaConsumer

from hexkit.base import InboundProviderBase
from hexkit.custom_types import JSON
from hexkit.eventsub.protocol import EventSubscriberProtocol


class KafkaEventSubscriber(InboundProviderBase):
    """Apache Kafka-specific event subscription provider."""

    def __init__(
        self,
        service_name: str,
        client_suffix: str,
        kafka_servers: list[str],
        translator: EventSubscriberProtocol,
        kafka_consumer_cls=KafkaConsumer,
    ):
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
            translator (EventSubscriberProto):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            kafka_consumer_cls:
                Overwrite the used Kafka Producer class. Only intented for unit testing.
        """

        client_id = f"{service_name}.{client_suffix}"
        topics = translator.topics
        self._types_whitelist = translator.event_types

        self._consumer = kafka_consumer_cls(
            *topics,
            bootstrap_servers=kafka_servers,
            client_id=client_id,
            group_id=service_name,
            auto_offset_reset="earliest",
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
        )

    def _consume_event(event:)

    def run(self) -> None:
        """Start consuming events and passing them down to the translator.
        This method blocks forever."""
        
        process_event = process_func_factory(
            self.exec_funcs, event_schemas=self.event_schemas
        )

        if run_forever:
            for event in self._consumer:
                process_event(event)
        else:
            event = next(self._consumer)
            process_event(event)

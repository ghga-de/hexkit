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
Apache Kafka-specific event publisher and subscriber provider.
They correspond to the `EventPublisherProtocol` and `EventSubscriberProtocol`, respectively.

Require dependencies of the `akafka` extra. See the `setup.cfg`.
"""
from typing import Protocol, Coroutine, Callable, Optional, Any
from abc import ABC, abstractmethod

import json

from aiokafka import AIOKafkaProducer

from hexkit.custom_types import JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol


class OutOfContextError(Exception):
    """Raised when a context manager is used out of context (i.e. not within a `with`
    block)."""


class EventTypeNotFoundError(RuntimeError):
    """Thrown when no `type` was set in the headers of an event."""


def generate_client_id(service_name: str, client_suffix: str) -> str:
    """
    Generate client id (from the perspective of the Kafka broker) by concatenating
    the service name and the client suffix.
    """
    return f"{service_name}.{client_suffix}"


class KafkaProducerCompatible(Protocol):
    """A protocol describing a AIOKafkaProducer or equivalent."""

    def __init__(
        self,
        bootstrap_servers: list[str],
        client_id: str,
        key_serializer: Callable[[Any], bytes],
        value_serializer: Callable[[Any], bytes],
    ):
        """
        Initialize the producer with some config params.

        Args:
            bootstrap_servers (list[str]):
                List of connection strings pointing to the kafka brokers.
            client_id (str):
                A globally unique ID identifying this the Kafka client.
            key_serializer:
                Function to serialize the keys into bytes.
            value_serializer:
                Function to serialize the values into bytes.
        """

        ...

    async def start(self):
        """Setup the producer."""
        ...

    async def stop(self):
        """Teardown the producer."""
        ...

    async def send_and_wait(self, topic, *, key, value, headers) -> None:
        """Send event."""
        ...


class KafkaEventPublisher(EventPublisherProtocol):
    """Apache Kafka specific event publishing provider."""

    def __init__(
        self,
        *,
        service_name: str,
        client_suffix: str,
        kafka_servers: list[str],
        kafka_producer_cls: type[KafkaProducerCompatible] = AIOKafkaProducer,
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
            kafka_producer_cls:
                Overwrite the used Kafka Producer class. Only intented for unit testing.
        """
        super().__init__()

        self._kafka_servers = kafka_servers
        self._kafka_producer_cls = kafka_producer_cls

        self._client_id = generate_client_id(service_name, client_suffix)
        self._producer: Optional[KafkaProducerCompatible] = None

    @staticmethod
    def ensure_context(method):
        """A decorator that can be added to methods to make sure that the instance is
        currently in context."""

        async def guarded_method(self: KafkaEventPublisher, *args, **kwargs):
            """Wraps method to ensure that it is executed in context."""
            if self._producer is None:  # pylint: disable=protected-access
                raise OutOfContextError()
            return await method(self, *args, **kwargs)

        return guarded_method

    async def __aenter__(self):
        """Initialize and stop the kafka producer."""

        self._producer = self._kafka_producer_cls(
            bootstrap_servers=self._kafka_servers,
            client_id=self._client_id,
            key_serializer=lambda key: key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )
        await self._producer.start()

    @ensure_context
    async def __aexit__(self, exc_type, exc_val, exc_trace):
        """Stop the kafka producer."""
        await self._producer.stop()

    @ensure_context
    async def publish(
        self, *, payload: JsonObject, type_: str, key: str, topic: str
    ) -> None:
        """Publish an event to an Apache Kafka event broker.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        await super().publish(payload=payload, type_=type_, key=key, topic=topic)

        event_headers = [("type", type_.encode("ascii"))]

        await self._producer.start()
        try:
            await self._producer.send_and_wait(
                topic, key=key, value=payload, headers=event_headers
            )
        finally:
            await self._producer.stop()

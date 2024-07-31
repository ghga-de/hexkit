# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
Apache Kafka-specific subscriber provider corresponding to the
`EventSubscriberProtocol`.

Require dependencies of the `akafka` extra. See the `setup.cfg`.
"""

import json
import logging
import ssl
from contextlib import asynccontextmanager
from typing import Callable, Literal, Optional, Protocol, TypeVar

from aiokafka import AIOKafkaConsumer

from hexkit.base import InboundProviderBase
from hexkit.correlation import set_correlation_id
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.utils import (
    generate_client_id,
    generate_ssl_context,
)


class EventHeaderNotFoundError(RuntimeError):
    """Thrown when a given detail was not set in the headers of an event."""

    def __init__(self, *, header_name):
        message = f"No '{header_name}' was set in event header."
        super().__init__(message)


class ConsumerEvent(Protocol):
    """Duck type of an event as received from a KafkaConsumerCompatible."""

    topic: str
    key: str
    value: JsonObject
    headers: list[tuple[str, bytes]]
    partition: int
    offset: int


def headers_as_dict(event: ConsumerEvent) -> dict[str, str]:
    """Extract the headers from a ConsumerEvent object and return them as a dict."""
    return {name: value.decode("ascii") for name, value in event.headers}


def get_header_value(header_name: str, headers: dict[str, str]) -> str:
    """Extract the given value from the dict headers and raise an error if not found."""
    try:
        return headers[header_name]
    except KeyError as err:
        raise EventHeaderNotFoundError(header_name=header_name) from err


KCC = TypeVar("KCC")


class KafkaConsumerCompatible(Protocol):
    """A python duck type protocol describing an AIOKafkaConsumer or equivalent."""

    def __init__(  # noqa: PLR0913
        self,
        *topics: Ascii,
        bootstrap_servers: str,
        security_protocol: str,
        ssl_context: Optional[ssl.SSLContext],
        client_id: str,
        group_id: str,
        auto_offset_reset: Literal["earliest"],
        enable_auto_commit: bool,
        key_deserializer: Callable[[bytes], str],
        value_deserializer: Callable[[bytes], str],
        max_partition_fetch_bytes: int,
    ):
        """
        Initialize the consumer with some config params.

        Args:
            bootstrap_servers:
                Comma-separated list of connection strings pointing to the kafka
                brokers.
            client_id:
                A globally unique ID identifying the Kafka client.
            group_id:
                An identifier for the consumer group.
            auto_offset_reset:
                Can be set to "earliest".
            key_serializer:
                Function to deserialize the keys into strings.
            value_serializer:
                Function to deserialize the values into strings.
            max_partition_fetch_bytes:
                Maximum receivable message size
        """
        ...

    async def commit(self, offsets=None):
        """Commit offsets to Kafka Broker."""
        ...

    async def start(self) -> None:
        """Setup the consumer."""
        ...

    async def stop(self) -> None:
        """Teardown the consumer."""
        ...

    def __aiter__(self: KCC) -> KCC:
        """Returns an async iterator for iterating through events."""
        ...

    async def __anext__(self) -> ConsumerEvent:
        """Used to get the next event."""
        ...


class KafkaEventSubscriber(InboundProviderBase):
    """Apache Kafka-specific event subscription provider."""

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        translator: EventSubscriberProtocol,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
    ):
        """
        Setup and teardown KafkaEventPublisher instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            kafka_consumer_cls:
                Overwrite the used Kafka consumer class. Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        topics = translator.topics_of_interest

        consumer = kafka_consumer_cls(
            *topics,
            bootstrap_servers=",".join(config.kafka_servers),
            security_protocol=config.kafka_security_protocol,
            ssl_context=generate_ssl_context(config),
            client_id=client_id,
            group_id=config.service_name,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
            max_partition_fetch_bytes=config.kafka_max_message_size,
        )
        try:
            await consumer.start()
            yield cls(consumer=consumer, translator=translator)
        finally:
            await consumer.stop()

    def __init__(
        self, *, consumer: KafkaConsumerCompatible, translator: EventSubscriberProtocol
    ):
        """Please do not call directly! Should be called by the `construct` method.
        Args:
            consumer:
                hands over a started AIOKafkaProducer.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
        """
        self._consumer = consumer
        self._translator = translator
        self._types_whitelist = translator.types_of_interest

    @staticmethod
    def _get_event_label(event: ConsumerEvent) -> str:
        """Get a label that identifies an event."""
        return (
            f"{event.topic} - {event.partition} - {event.key} - {event.offset}"
            + " (topic-partition-offset)"
        )

    async def _consume_event(self, event: ConsumerEvent) -> None:
        """Consume an event by passing it down to the translator via the protocol."""
        event_label = self._get_event_label(event)
        headers = headers_as_dict(event)

        try:
            type_ = get_header_value(header_name="type", headers=headers)
            correlation_id = get_header_value(
                header_name="correlation_id", headers=headers
            )
        except EventHeaderNotFoundError as err:
            logging.warning("Ignored an event: %s. %s", event_label, err.args[0])
            # acknowledge event receipt
            await self._consumer.commit()
            return

        if type_ in self._types_whitelist:
            logging.info('Consuming event of type "%s": %s', type_, event_label)

            try:
                async with set_correlation_id(correlation_id):
                    # blocks until event processing is completed:
                    await self._translator.consume(
                        payload=event.value,
                        type_=type_,
                        topic=event.topic,
                        key=event.key,
                    )
                    # acknowledge successfully processed event
                    await self._consumer.commit()
            except Exception:
                logging.error(
                    "A fatal error occurred while processing the event: %s",
                    event_label,
                )
                raise

        else:
            logging.info("Ignored event of type %s: %s", type_, event_label)
            # acknowledge event receipt
            await self._consumer.commit()

    async def run(self, forever: bool = True) -> None:
        """
        Start consuming events and passing them down to the translator.
        By default, it blocks forever.
        However, you can set `forever` to `False` to make it return after handling one
        event.
        """
        if forever:
            async for event in self._consumer:
                await self._consume_event(event)
        else:
            event = await self._consumer.__anext__()
            await self._consume_event(event)

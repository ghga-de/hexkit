# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

import json
import logging
import ssl
from contextlib import asynccontextmanager
from typing import Any, Callable, Literal, Optional, Protocol, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

from hexkit.base import InboundProviderBase
from hexkit.correlation import (
    CorrelationIdContextError,
    get_correlation_id,
    new_correlation_id,
    set_correlation_id,
    validate_correlation_id,
)
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol

__all__ = [
    "KafkaConfig",
    "KafkaEventPublisher",
    "ConsumerEvent",
    "KafkaEventSubscriber",
]


class KafkaConfig(BaseSettings):
    """Config parameters needed for connecting to Apache Kafka."""

    service_name: str = Field(
        ...,
        examples=["my-cool-special-service"],
        description="The name of the (micro-)service from which messages are published.",
    )
    service_instance_id: str = Field(
        ...,
        examples=["germany-bw-instance-001"],
        description=(
            "A string that uniquely identifies this instance across all instances of"
            + " this service. A globally unique Kafka client ID will be created by"
            + " concatenating the service_name and the service_instance_id."
        ),
    )
    kafka_servers: list[str] = Field(
        ...,
        examples=[["localhost:9092"]],
        description="A list of connection strings to connect to Kafka bootstrap servers.",
    )
    kafka_security_protocol: Literal["PLAINTEXT", "SSL"] = Field(
        default="PLAINTEXT",
        description="Protocol used to communicate with brokers. "
        + "Valid values are: PLAINTEXT, SSL.",
    )
    kafka_ssl_cafile: str = Field(
        default="",
        description="Certificate Authority file path containing certificates"
        + " used to sign broker certificates. If a CA is not specified, the default"
        + " system CA will be used if found by OpenSSL.",
    )
    kafka_ssl_certfile: str = Field(
        default="",
        description="Optional filename of client certificate, as well as any"
        + " CA certificates needed to establish the certificate's authenticity.",
    )
    kafka_ssl_keyfile: str = Field(
        default="", description="Optional filename containing the client private key."
    )
    kafka_ssl_password: SecretStr = Field(
        default="",
        description="Optional password to be used for the client private key.",
    )
    generate_correlation_id: bool = Field(
        default=True,
        examples=[True, False],
        description=(
            "A flag, which, if False, will result in an error when trying to publish an"
            + " event without a valid correlation ID set for the context. If True, the a"
            + " newly correlation ID will be generated and used in the event header."
        ),
    )


class EventHeaderNotFoundError(RuntimeError):
    """Thrown when a given detail was not set in the headers of an event."""

    def __init__(self, *, header_name):
        message = f"No '{header_name}' was set in event header."
        super().__init__(message)


def generate_client_id(*, service_name: str, instance_id: str) -> str:
    """
    Generate client id (from the perspective of the Kafka broker) by concatenating
    the service name and the client suffix.
    """
    return f"{service_name}.{instance_id}"


def generate_ssl_context(config: KafkaConfig) -> Optional[ssl.SSLContext]:
    """Generate SSL context for an encrypted SSL connection to Kafka broker."""
    return (
        create_ssl_context(
            cafile=config.kafka_ssl_cafile,
            certfile=config.kafka_ssl_certfile,
            keyfile=config.kafka_ssl_keyfile,
            password=config.kafka_ssl_password.get_secret_value(),
        )
        if config.kafka_security_protocol == "SSL"
        else None
    )


class KafkaProducerCompatible(Protocol):
    """A python duck type protocol describing an AIOKafkaProducer or equivalent."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        bootstrap_servers: str,
        security_protocol: str,
        ssl_context: Optional[ssl.SSLContext],
        client_id: str,
        key_serializer: Callable[[Any], bytes],
        value_serializer: Callable[[Any], bytes],
    ):
        """
        Initialize the producer with some config params.
        Args:
            bootstrap_servers:
                Comma-separated list of connection strings pointing to the kafka
                brokers.
            client_id:
                A globally unique ID identifying the Kafka client.
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

    async def send_and_wait(self, topic, *, key, value, headers) -> Any:
        """Send event."""
        ...


class KafkaEventPublisher(EventPublisherProtocol):
    """Apache Kafka specific event publishing provider."""

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        kafka_producer_cls: type[KafkaProducerCompatible] = AIOKafkaProducer,
    ):
        """
        Setup and teardown KafkaEventPublisher instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            kafka_producer_cls:
                Overwrite the used Kafka Producer class. Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        producer = kafka_producer_cls(
            bootstrap_servers=",".join(config.kafka_servers),
            security_protocol=config.kafka_security_protocol,
            ssl_context=generate_ssl_context(config),
            client_id=client_id,
            key_serializer=lambda key: key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

        try:
            await producer.start()
            yield cls(
                producer=producer,
                generate_correlation_id=config.generate_correlation_id,
            )
        finally:
            await producer.stop()

    def __init__(
        self,
        *,
        producer: KafkaProducerCompatible,
        generate_correlation_id: bool,
    ):
        """Please do not call directly! Should be called by the `construct` method.
        Args:
            producer:
                hands over a started AIOKafkaProducer.
            generate_correlation_id:
                whether to throw an error or generate a new ID if no correlation ID is
                set for the context when trying to publish an event. An invalid ID will
                result in an error in either case.
        """
        self._producer = producer
        self._generate_correlation_id = generate_correlation_id

    async def _publish_validated(
        self, *, payload: JsonObject, type_: Ascii, key: Ascii, topic: Ascii
    ) -> None:
        """Publish an event with already validated topic and type.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        try:
            correlation_id = get_correlation_id()
        except CorrelationIdContextError:
            if not self._generate_correlation_id:
                raise

            correlation_id = new_correlation_id()
            logging.info("Generated new correlation ID: %s", correlation_id)

        validate_correlation_id(correlation_id)

        event_headers = [
            ("type", type_.encode("ascii")),
            ("correlation_id", correlation_id.encode("ascii")),
        ]
        await self._producer.send_and_wait(
            topic, key=key, value=payload, headers=event_headers
        )


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
        """Returns an async iterator for iterating through events."""  #
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
            f"{event.topic} - {event.partition} - {event.offset} "
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

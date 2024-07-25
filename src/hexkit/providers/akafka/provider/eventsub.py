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
from dataclasses import dataclass
from typing import Callable, Literal, Optional, Protocol, TypeVar

from aiokafka import AIOKafkaConsumer

from hexkit.base import InboundProviderBase
from hexkit.correlation import set_correlation_id
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.utils import (
    generate_client_id,
    generate_ssl_context,
)

ORIGINAL_TOPIC_FIELD = "_original_topic"


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


@dataclass
class ExtractedEvent:
    """A class encapsulating the data extracted from a `ConsumerEvent` instance."""

    topic: Ascii
    type_: Ascii
    payload: JsonObject
    key: Ascii


def get_event_label(event: ConsumerEvent) -> str:
    """Make a label that identifies an event."""
    return (
        f"{event.topic} - {event.partition} - {event.key} - {event.offset}"
        + " (topic-partition-key-offset)"
    )


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


class OriginalTopicError(RuntimeError):
    """Raised when the original topic is missing from the event."""

    def __init__(self, *, event_label: str):
        super().__init__(f"Unable to get original topic from event: {event_label}.")


class KafkaEventSubscriber(InboundProviderBase):
    """Apache Kafka-specific event subscription provider."""

    class RetriesExhaustedError(RuntimeError):
        """Raised when an event has been retried the maximum number of times."""

        def __init__(self, *, event_type: str):
            message = f"The maximum number of retries has been reached for '{
                event_type}' event."
            super().__init__(message)

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        translator: EventSubscriberProtocol,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
        publisher: Optional[EventPublisherProtocol] = None,
    ):
        """
        Setup and teardown KafkaRetrySubscriber instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            publisher:
                running instance of publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher. Can be None if
                not using the dead letter queue.
            kafka_consumer_cls:
                Overwrite the used Kafka consumer class. Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        topics = translator.topics_of_interest

        if config.kafka_enable_dlq:
            if publisher is None:
                error = ValueError("A publisher is required when the DLQ is enabled.")
                logging.error(error)
                raise error
            topics.append(config.kafka_retry_topic)

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
            yield cls(
                consumer=consumer,
                translator=translator,
                publisher=publisher,
                config=config,
            )
        finally:
            await consumer.stop()

    def __init__(
        self,
        *,
        consumer: KafkaConsumerCompatible,
        translator: EventSubscriberProtocol,
        config: KafkaConfig,
        publisher: Optional[EventPublisherProtocol] = None,
    ):
        """Please do not call directly! Should be called by the `construct` method.
        Args:
            consumer:
                hands over a started AIOKafkaConsumer.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            publisher:
                running instance of publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher. Can be None if
                not using the dead letter queue.
            config:
                the KafkaRetryConfig instance containing dlq topics and retry allowance.
        """
        self._consumer = consumer
        self._translator = translator
        self._types_whitelist = translator.types_of_interest
        self._publisher = publisher
        self._dlq_topic = config.kafka_dlq_topic
        self._dlq_retry_topic = config.kafka_retry_topic
        self._max_retries = config.kafka_max_retries
        self._enable_dlq = config.kafka_enable_dlq

    def _get_original_topic(self, event: ConsumerEvent) -> str:
        """Get the topic to use -- either the given topic or the one from `_original_topic`.

        Raises `OriginalTopicError` if the original topic name is missing for events in
        the retry topic.
        """
        topic = event.topic
        if topic == self._dlq_retry_topic:
            topic = str(event.value.get(ORIGINAL_TOPIC_FIELD, ""))
            if not topic:
                event_label = get_event_label(event)
                error = OriginalTopicError(event_label=event_label)
                logging.error(error)
                raise error
            logging.info(
                "Received previously failed event from topic '%s' for retry.", topic
            )
        return topic

    async def _publish_to_dlq(self, *, event: ExtractedEvent):
        """Publish the event to the DLQ topic."""
        dlq_payload = {**event.payload, ORIGINAL_TOPIC_FIELD: event.topic}
        logging.debug("Publishing failed event to DLQ topic '%s'.", self._dlq_topic)
        await self._publisher.publish(  # type: ignore
            payload=dlq_payload,
            type_=event.type_,
            topic=self._dlq_topic,
            key=event.key,
        )
        logging.info("Published event to DLQ topic '%s'", self._dlq_topic)

    async def _retry_event(self, *, event: ExtractedEvent, retries_left: int):
        """Retry the event until the maximum number of retries is reached."""
        retries_left -= 1
        try:
            logging.info(
                "Retrying event of type '%s' on topic '%s' with key '%s'.",
                extra={
                    "type": event.type_,
                    "topic": event.topic,
                    "key": event.key,
                    "retries_left": retries_left,
                },
            )
            await self._translator.consume(
                payload=event.payload,
                type_=event.type_,
                topic=event.topic,
                key=event.key,
            )
        except Exception as err:
            if retries_left > 0:
                await self._retry_event(event=event, retries_left=retries_left)
            else:
                raise self.RetriesExhaustedError(event_type=event.type_) from err

    async def _handle_consumption(self, *, event: ExtractedEvent):
        """Try to pass the event to the consumer.

        If the event fails:
        1. Retry until retries are exhausted, if retries are configured.
        2. Publish the event to the DLQ topic if the DLQ is enabled. Done afterward.
           or
        3. Allow failure with unhandled error if DLQ is not configured.
        """
        try:
            await self._translator.consume(
                payload=event.payload,
                type_=event.type_,
                topic=event.topic,
                key=event.key,
            )
        except Exception:
            logging.warning(
                "Failed initial attempt to consume event of type '%s' on topic '%s' with key '%s'.",
                event.type_,
                event.topic,
                event.key,
            )
            if self._max_retries > 0:
                # Don't raise RetriesExhaustedError unless retries are actually attempted
                try:
                    await self._retry_event(event=event, retries_left=self._max_retries)
                except self.RetriesExhaustedError:
                    if self._enable_dlq:
                        await self._publish_to_dlq(event=event)
                    else:
                        raise
            elif self._enable_dlq:
                await self._publish_to_dlq(event=event)
            else:
                raise  # re-raise Exception

    async def _consume_event(self, event: ConsumerEvent) -> None:
        """Consume an event by passing it down to the translator via the protocol."""
        event_label = get_event_label(event)
        headers = headers_as_dict(event)

        try:
            type_ = get_header_value(header_name="type", headers=headers)
            correlation_id = get_header_value(
                header_name="correlation_id", headers=headers
            )
        except EventHeaderNotFoundError as err:
            logging.warning("Ignored an event: %s.  %s", event_label, err.args[0])
            # acknowledge event receipt
            await self._consumer.commit()
            return

        # If the event is from the DLQ retry topic, consume with the original topic
        topic = self._get_original_topic(event=event)
        payload = event.value
        if topic != event.topic:
            payload = {
                k: v for k, v in event.value.items() if k != ORIGINAL_TOPIC_FIELD
            }

        if type_ in self._types_whitelist:
            logging.info('Consuming event of type "%s": %s', type_, event_label)
            extracted_event = ExtractedEvent(
                type_=type_, topic=topic, payload=payload, key=event.key
            )
            try:
                async with set_correlation_id(correlation_id):
                    await self._handle_consumption(event=extracted_event)
            except Exception:
                logging.critical(
                    "An error occurred while processing the event: %s. It was NOT"
                    " placed in the DLQ topic ('%s')",
                    event_label,
                    self._dlq_topic,
                )
                raise
            else:
                # Only save consumed event offsets if it was successful or sent to DLQ
                await self._consumer.commit()

        else:
            logging.info("Ignored event of type %s: %s", type_, event_label)

            # Always acknowledge event receipt for ignored events
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


class KafkaDLQSubscriber(InboundProviderBase):
    """A kafka event subscriber that subscribes to the configured DLQ topic and either
    discards each event or publishes it to the retry topic as instructed.
    """

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
        publisher: EventPublisherProtocol,
    ):
        """
        Setup and teardown KafkaEventPublisher instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            publisher:
                running instance of publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher.
            kafka_consumer_cls:
                Overwrite the used Kafka consumer class . Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        consumer = kafka_consumer_cls(
            config.kafka_dlq_topic,
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
            yield cls(
                consumer=consumer,
                publisher=publisher,
                dlq_retry_topic=config.kafka_retry_topic,
            )
        finally:
            await consumer.stop()

    def __init__(
        self,
        *,
        consumer: KafkaConsumerCompatible,
        publisher: EventPublisherProtocol,
        dlq_retry_topic: str,
    ):
        """Please do not call directly! Should be called by the `construct` method.
        Args:
            consumer:
                hands over a started AIOKafkaConsumer.
            publisher:
                running instance of publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher.
            dlq_retry_topic:
                The name of the topic used to requeue failed events.
        """
        self._consumer = consumer
        self._publisher = publisher
        self._dlq_retry_topic = dlq_retry_topic

    async def _publish_to_retry(self, event: ConsumerEvent):
        """Publish the event to the retry topic."""
        event_label = get_event_label(event)
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

        original_topic = event.value.get(ORIGINAL_TOPIC_FIELD, "")
        if not original_topic:
            error = RuntimeError(
                "Tried to publish event to the retry topic, but the original topic was"
                + " not found in the payload. This should be populated automatically."
            )
            logging.critical(error, extra={"payload": event.value})
            raise error

        async with set_correlation_id(correlation_id):
            await self._publisher.publish(
                payload=event.value,
                type_=type_,
                topic=self._dlq_retry_topic,
                key=event.key,
            )
            logging.info(
                "Published an event to the retry topic '%s'", self._dlq_retry_topic
            )

    async def run(self, ignore: bool = False) -> None:
        """
        Start consuming events and passing them down to the translator.
        It will return after handling one event.
        If `ignore` is True, the event will be ignored.
        Otherwise, the event will be published to the retry topic.
        """
        event = await self._consumer.__anext__()
        if not ignore:
            await self._publish_to_retry(event)

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

import asyncio
import json
import logging
import ssl
from collections.abc import Awaitable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Callable, Literal, Optional, Protocol, TypeVar, cast

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

ORIGINAL_TOPIC_FIELD = "original_topic"
EXC_CLASS_FIELD = "exc_class"
EXC_MSG_FIELD = "exc_msg"


class ConsumerEvent(Protocol):
    """Duck type of an event as received from a KafkaConsumerCompatible."""

    topic: str
    key: str
    value: JsonObject
    headers: list[tuple[str, bytes]]
    partition: int
    offset: int


@dataclass
class ExtractedEventInfo:
    """A class encapsulating the data extracted from a `ConsumerEvent`-like object.

    This data includes the topic, type, payload, and key of the event.
    """

    topic: Ascii
    type_: Ascii
    payload: JsonObject
    key: Ascii
    headers: dict[str, str]

    def __init__(self, event: Optional[ConsumerEvent] = None, **kwargs):
        """Initialize an instance of ExtractedEventInfo."""
        self.topic = kwargs.get("topic", event.topic if event else "")
        self.payload = kwargs.get("payload", event.value if event else "")
        self.key = kwargs.get("key", event.key if event else "")
        self.headers = kwargs.get("headers", headers_as_dict(event) if event else {})
        self.headers = cast(dict, self.headers)
        self.type_ = kwargs.get("type_", self.headers.pop("type", ""))

    @property
    def encoded_headers(self) -> list[tuple[str, bytes]]:
        """Return the headers as a list of 2-tuples with the values encoded as bytes."""
        return [(name, value.encode("ascii")) for name, value in self.headers.items()]


def service_name_from_dlq_topic(dlq_topic: str) -> str:
    """Extract the service name from a DLQ topic name."""
    return dlq_topic.rsplit(".", 1)[1].removesuffix("-dlq")


def get_event_label(event: ConsumerEvent) -> str:
    """Make a label that identifies an event."""
    return (
        f"{event.topic} - {event.partition} - {event.key} - {event.offset}"
        + " (topic-partition-key-offset)"
    )


def headers_as_dict(event: ConsumerEvent) -> dict[str, str]:
    """Extract the headers from a ConsumerEvent object and return them as a dict."""
    return {name: value.decode("ascii") for name, value in event.headers}


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

    class RetriesExhaustedError(RuntimeError):
        """Raised when an event has been retried the maximum number of times."""

        def __init__(self, *, event_type: str, max_retries: int):
            msg = f"All retries (total of {max_retries}) exhausted for '{event_type}' event."
            super().__init__(msg)

    class RetriesLeftError(ValueError):
        """Raised when the value for `retries_left` is invalid."""

        def __init__(self, *, retries_left: int, max_retries: int):
            msg = (
                f"Invalid value for retries_left: {retries_left} (should be between"
                + f" 1 and {max_retries}, inclusive)."
            )
            super().__init__(msg)

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        translator: EventSubscriberProtocol,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
        dlq_publisher: Optional[EventPublisherProtocol] = None,
    ):
        """
        Setup and teardown KafkaEventSubscriber instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            dlq_publisher:
                A running instance of a publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher. Can be None if
                not using the dead letter queue. It is used to publish events to the DLQ.
            kafka_consumer_cls:
                Overwrite the used Kafka consumer class. Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        topics = translator.topics_of_interest

        if config.kafka_enable_dlq:
            topics.append(config.service_name + "-retry")
            if dlq_publisher is None:
                error = ValueError("A publisher is required when the DLQ is enabled.")
                logging.error(error)
                raise error

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

        await consumer.start()
        try:
            yield cls(
                consumer=consumer,
                translator=translator,
                dlq_publisher=dlq_publisher,
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
        dlq_publisher: Optional[EventPublisherProtocol] = None,
    ):
        """Please do not call directly! Should be called by the `construct` method.
        Args:
            consumer:
                hands over a started AIOKafkaConsumer.
            translator (EventSubscriberProtocol):
                The translator that translates between the protocol (mentioned in the
                type annotation) and an application-specific port
                (according to the triple hexagonal architecture).
            dlq_publisher:
                A running instance of a publishing provider that implements the
                EventPublisherProtocol, such as KafkaEventPublisher. Can be None if
                not using the dead letter queue. It is used to publish events to the DLQ.
            config:
                The KafkaConfig instance
        """
        self._consumer = consumer
        self._translator = translator
        self._types_whitelist = translator.types_of_interest
        self._dlq_publisher = dlq_publisher
        self._dlq_suffix = f".{config.service_name}-dlq"
        self._retry_topic = config.service_name + "-retry"
        self._max_retries = config.kafka_max_retries
        self._enable_dlq = config.kafka_enable_dlq
        self._retry_backoff = config.kafka_retry_backoff

    async def _publish_to_dlq(self, *, event: ExtractedEventInfo, exc: Exception):
        """Publish the event to the corresponding DLQ topic.

        The exception instance is included in the headers, but is split into the class
        name and the string representation of the exception.

        Args
        - `event`: The event to publish to the DLQ.
        - `exc`: The exception that caused the event to be published to the DLQ.
        """
        dlq_topic = event.topic + self._dlq_suffix
        logging.debug("About to publish an event to DLQ topic '%s'", dlq_topic)
        await self._dlq_publisher.publish(  # type: ignore
            payload=event.payload,
            type_=event.type_,
            topic=dlq_topic,
            key=event.key,
            headers={
                EXC_CLASS_FIELD: exc.__class__.__name__,
                EXC_MSG_FIELD: str(exc),
            },
        )
        logging.info("Published event to DLQ topic '%s'", dlq_topic)

    async def _retry_event(self, *, event: ExtractedEventInfo, retries_left: int):
        """Retry the event until the maximum number of retries is reached.

        If the retry fails, this method is called again with `retries_left` decremented.

        Raises:
        - `RetriesExhaustedError`: If all retries are exhausted without success.
        - `RetriesLeftError`: If the value for `retries_left` is invalid.
        """
        # Check if retries_left is valid.
        if not 0 < retries_left <= self._max_retries:
            error = self.RetriesLeftError(
                retries_left=retries_left, max_retries=self._max_retries
            )
            logging.error(error)
            raise error

        # Decrement retries_left and calculate backoff time, then wait and retry.
        retries_left -= 1
        retry_number = self._max_retries - retries_left
        backoff_time = self._retry_backoff * 2 ** (retry_number - 1)
        try:
            logging.info(
                "Retry %i of %i for event of type '%s' on topic '%s' with key '%s',"
                + " beginning in %i seconds.",
                retry_number,
                self._max_retries,
                event.type_,
                event.topic,
                event.key,
                backoff_time,
            )
            await asyncio.sleep(backoff_time)
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
                raise self.RetriesExhaustedError(
                    event_type=event.type_, max_retries=self._max_retries
                ) from err

    async def _handle_consumption(self, *, event: ExtractedEventInfo):
        """Try to pass the event to the consumer.

        If the event fails:
        1. Retry until retries are exhausted, if retries are configured.
        2. Publish event to DLQ topic if the DLQ is enabled. Done afterward.
           or
        3. Allow failure with unhandled error if the DLQ is not enabled. This is the
           pre-DLQ behavior.
        """
        try:
            await self._translator.consume(
                payload=event.payload,
                type_=event.type_,
                topic=event.topic,
                key=event.key,
            )
        except Exception as underlying_error:
            logging.warning(
                "Failed initial attempt to consume event of type '%s' on topic '%s' with key '%s'.",
                event.type_,
                event.topic,
                event.key,
            )

            if not self._max_retries:
                if not self._enable_dlq:
                    raise  # re-raise Exception
                await self._publish_to_dlq(event=event, exc=underlying_error)
                return

            # Don't raise RetriesExhaustedError unless retries are actually attempted
            try:
                await self._retry_event(event=event, retries_left=self._max_retries)
            except (self.RetriesExhaustedError, self.RetriesLeftError) as retry_error:
                # If the value for retries_left was invalid, we still want to handle it
                # the same way as if all retries were exhausted. The separate error is
                # for better traceability.
                logging.warning(retry_error)
                if not self._enable_dlq:
                    raise retry_error from underlying_error
                await self._publish_to_dlq(event=event, exc=underlying_error)

    def _extract_info(self, event: ConsumerEvent) -> ExtractedEventInfo:
        """Validate the event, returning the extracted info."""
        event_info = ExtractedEventInfo(event)
        if event_info.topic == self._retry_topic:
            # The event is being consumed by from the retry topic, so we expect the
            # original topic to be in the headers.
            event_info.topic = event_info.headers.get(ORIGINAL_TOPIC_FIELD, "")
            logging.info(
                "Received previously failed event from topic '%s' for retry.",
                event_info.topic,
            )
        elif event_info.topic.endswith(self._dlq_suffix):
            # The event is being consumed from a DLQ topic, so we remove the DLQ suffix
            # to produce the original topic name.
            original_topic = event_info.topic.removesuffix(self._dlq_suffix)
            logging.info(
                "Received event from DLQ topic '%s' for processing. Original topic: '%s'",
                event_info.topic,
                original_topic,
            )
            event_info.topic = original_topic
        return event_info

    def _validate_extracted_info(self, event: ExtractedEventInfo):
        """Extract and validate the event, returning the correlation ID and the extracted info."""
        dlq_topic = event.topic + self._dlq_suffix
        correlation_id = event.headers.get("correlation_id", "")
        errors = []
        if not event.type_:
            errors.append("event type is empty")
        elif event.type_ not in self._types_whitelist:
            errors.append(f"event type '{event.type_}' is not in the whitelist")
        if not correlation_id:
            errors.append("correlation_id is empty")
        if event.topic in (self._retry_topic, dlq_topic):
            errors.append(
                f"original_topic header cannot be {self._retry_topic} or"
                + f" {dlq_topic}. Value: '{event.topic}'"
            )
        elif not event.topic:
            errors.append(
                "topic is empty"
            )  # only occurs if original_topic header is empty
        if errors:
            error = RuntimeError(", ".join(errors))
            raise error

    async def _consume_event(self, event: ConsumerEvent) -> None:
        """Consume an event by passing it down to the translator via the protocol."""
        event_label = get_event_label(event)
        event_info = self._extract_info(event)

        try:
            self._validate_extracted_info(event_info)
        except RuntimeError as err:
            logging.info(
                "Ignored event of type '%s': %s, errors: %s",
                event_info.type_,
                event_label,
                str(err),
            )
            # Always acknowledge event receipt for ignored events
            await self._consumer.commit()
            return

        try:
            logging.info(
                "Consuming event of type '%s': %s", event_info.type_, event_label
            )
            correlation_id = event_info.headers["correlation_id"]
            async with set_correlation_id(correlation_id):
                await self._handle_consumption(event=event_info)
        except Exception:
            # Errors only bubble up here if the DLQ isn't used
            dlq_topic = event_info.topic + self._dlq_suffix
            logging.critical(
                "An error occurred while processing event of type '%s': %s. It was NOT"
                " placed in the DLQ topic (%s)",
                event_info.type_,
                event_label,
                dlq_topic if self._enable_dlq else "DLQ is disabled",
            )
            raise
        else:
            # Only save consumed event offsets if it was successful or sent to DLQ
            await self._consumer.commit()

    async def run(self, forever: bool = True) -> None:
        """Start consuming events and passing them down to the translator.

        By default, this method blocks forever. However, you can set `forever`
        to `False` to make it return after handling one event.
        """
        if forever:
            async for event in self._consumer:
                await self._consume_event(event)
        else:
            event = await self._consumer.__anext__()
            await self._consume_event(event)


# Define function signature for the DLQ processor.
DLQEventProcessor = Callable[[ConsumerEvent], Awaitable[Optional[ExtractedEventInfo]]]


class DLQValidationError(RuntimeError):
    """Raised when an event from the DLQ fails validation."""

    def __init__(self, *, event: ConsumerEvent, reason: str):
        msg = f"DLQ Event '{get_event_label(event)}' is invalid: {reason}"
        super().__init__(msg)


class DLQProcessingError(RuntimeError):
    """Raised when an error occurs while processing an event from the DLQ."""

    def __init__(self, *, event: ConsumerEvent, reason: str):
        msg = f"DLQ Event '{get_event_label(event)}' cannot be processed: {reason}"
        super().__init__(msg)


def validate_dlq_headers(event: ConsumerEvent) -> None:
    """Validate the headers that should be populated on every DLQ event.

    Raises:
    - `DLQValidationError`: If any headers are determined to be invalid.
    """
    headers = headers_as_dict(event)
    expected_headers = [
        "type",
        "correlation_id",
        EXC_CLASS_FIELD,
        EXC_MSG_FIELD,
    ]
    invalid_headers = [key for key in expected_headers if not headers.get(key)]
    if invalid_headers:
        error_msg = f"Missing or empty headers: {', '.join(invalid_headers)}"
        raise DLQValidationError(event=event, reason=error_msg)


async def process_dlq_event(event: ConsumerEvent) -> Optional[ExtractedEventInfo]:
    """
    Simple 'processing' function for a message from a dead-letter queue that
    adheres to the DLQEventProcessor callable definition.

    Args:
    - `event`: The event to process.

    Returns:
    - `ConsumerEvent`: The unaltered event to publish to the retry topic.
    - `None`: A signal to discard the event.

    Raises:
    - `DLQValidationError`: If the event headers are invalid.
    """
    validate_dlq_headers(event)
    return ExtractedEventInfo(event)


class KafkaDLQSubscriber:
    """A kafka event subscriber that subscribes to the specified DLQ topic and either
    discards each event or publishes it to the retry topic as instructed.
    Further processing before requeuing is provided by a callable adhering to the
    DLQEventProcessor definition.
    """

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        dlq_topic: str,
        dlq_publisher: EventPublisherProtocol,
        process_dlq_event: DLQEventProcessor = process_dlq_event,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
    ):
        """
        Setup and teardown KafkaDLQSubscriber instance.

        Args:
        - `config`:
            Config parameters needed for connecting to Apache Kafka.
        - `dlq_topic`:
            The name of the DLQ topic to subscribe to. Has the format
            "{original_topic}.{service_name}-dlq".
        - `dlq_publisher`:
            A running instance of a publishing provider that implements the
            EventPublisherProtocol, such as KafkaEventPublisher. It is used to publish
            events to the retry topic.
        - `process_dlq_event`:
            An async callable adhering to the DLQEventProcessor definition that provides
            validation and processing for events from the DLQ. It should return _either_
            the event to publish to the retry topic (which may be altered) or `None` to
            discard the event. The `KafkaDLQSubscriber` will log and interpret
            `DLQValidationError` as a signal to discard/ignore the event, and all other
            errors will be re-raised as a `DLQProcessingError`.
        - `kafka_consumer_cls`:
            Overwrite the used Kafka consumer class. Only intended for unit testing.
        """
        client_id = generate_client_id(
            service_name=config.service_name, instance_id=config.service_instance_id
        )

        consumer = kafka_consumer_cls(
            dlq_topic,
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

        await consumer.start()
        try:
            yield cls(
                dlq_topic=dlq_topic,
                dlq_publisher=dlq_publisher,
                consumer=consumer,
                process_dlq_event=process_dlq_event,
            )
        finally:
            await consumer.stop()

    def __init__(
        self,
        *,
        dlq_topic: str,
        dlq_publisher: EventPublisherProtocol,
        consumer: KafkaConsumerCompatible,
        process_dlq_event: DLQEventProcessor,
    ):
        """Please do not call directly! Should be called by the `construct` method.

        Args:
        - `dlq_topic`:
            The name of the DLQ topic to subscribe to. Has the format
            "{original_topic}.{service_name}-dlq".
        - `dlq_publisher`:
            A running instance of a publishing provider that implements the
            EventPublisherProtocol, such as KafkaEventPublisher.
        - `consumer`:
            hands over a started AIOKafkaConsumer.
        - `process_dlq_event`:
            An async callable adhering to the DLQEventProcessor definition that provides
            validation and processing for events from the DLQ. It should return _either_
            the event to publish to the retry topic (which may be altered) or `None` to
            discard the event. The `KafkaDLQSubscriber` will log and interpret
            `DLQValidationError` as a signal to discard/ignore the event, and all other
            errors will be re-raised as a `DLQProcessingError`.
        """
        self._consumer = consumer
        self._publisher = dlq_publisher
        self._dlq_topic = dlq_topic

        # In KafkaEventSubscriber, we get the service name from the config. However,
        # the service name in effect here probably differs -- e.g., a DLQ-specific
        # service might be running the KafkaDLQSubscriber. So we extract the service
        # name from the DLQ topic name instead.
        service_name = service_name_from_dlq_topic(dlq_topic)
        self._retry_topic = service_name + "-retry"
        self._process_dlq_event = process_dlq_event

    async def _publish_to_retry(self, *, event: ExtractedEventInfo) -> None:
        """Publish the event to the retry topic."""
        correlation_id = event.headers["correlation_id"]

        async with set_correlation_id(correlation_id):
            await self._publisher.publish(
                payload=event.payload,
                type_=event.type_,
                key=event.key,
                topic=self._retry_topic,
                headers={ORIGINAL_TOPIC_FIELD: self._dlq_topic.rsplit(".", 1)[0]},
            )
            logging.info(
                "Published an event with type '%s' to the retry topic '%s'",
                event.type_,
                self._retry_topic,
            )

    async def _handle_dlq_event(self, *, event: ConsumerEvent) -> None:
        """Process an event from the dead-letter queue.

        The event is processed by `_process_dlq_event`, which validates the event
        and determines whether to publish it to the retry topic or discard it.
        """
        try:
            event_to_publish = await self._process_dlq_event(event)
        except DLQValidationError as err:
            logging.error("Ignoring event from DLQ due to validation failure: %s", err)
            return

        if event_to_publish:
            await self._publish_to_retry(event=event_to_publish)

    async def _ignore_event(self, event: ConsumerEvent) -> None:
        """Ignore the event, log it, and commit offsets"""
        event_label = get_event_label(event)
        logging.info(
            "Ignoring event from DLQ topic '%s': %s",
            self._dlq_topic,
            event_label,
        )
        await self._consumer.commit()

    async def ignore(self) -> None:
        """Directly ignore the next event from the DLQ topic."""
        event = await self._consumer.__anext__()
        event_label = get_event_label(event)
        logging.info(
            "Ignoring event from DLQ topic '%s': %s",
            self._dlq_topic,
            event_label,
        )
        await self._consumer.commit()

    async def preview(self, limit: int = 1, skip: int = 0) -> list[ExtractedEventInfo]:
        """Fetch the next events from the configured DLQ topic without processing them.

        Offsets are reset to their original positions after fetching. The pagination
        parameters 'skip' and 'limit' can be used to control the number of events
        returned and from how deep in the topic to start fetching.

        Args
        - `limit`: The maximum number of events to fetch.
        - `skip`: The number of events to skip before fetching.
        """
        topic_partitions = [
            topic_partition
            for topic_partition in self._consumer.assignment()  # type: ignore
            if topic_partition.topic == self._dlq_topic
        ]

        positions = {
            topic_partition: await self._consumer.position(  # type: ignore
                topic_partition
            )
            for topic_partition in topic_partitions
        }

        max_records = limit + skip
        fetched = await self._consumer.getmany(  # type: ignore
            timeout_ms=500,
            max_records=max_records,
        )

        # Convert results to list
        events = []
        with suppress(StopIteration):
            events = next(iter(fetched.values()))
            events = events[skip:]

        # Reset the consumer to the original offsets
        for partition, offset in positions.items():
            self._consumer.seek(partition, offset)  # type: ignore

        return [ExtractedEventInfo(event) for event in events]

    async def process(self) -> None:
        """Process the next event from the configured DLQ topic.

        Validate and resolve the event based on custom logic, if applicable.
        """
        event = await self._consumer.__anext__()
        try:
            await self._handle_dlq_event(event=event)
            await self._consumer.commit()
        except Exception as exc:
            error = DLQProcessingError(event=event, reason=str(exc))
            logging.critical(
                "Failed to process event from DLQ topic '%s': '%s'",
                self._dlq_topic,
                exc,
            )
            raise error from exc

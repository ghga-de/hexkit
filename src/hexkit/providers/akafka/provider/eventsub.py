# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
from collections import defaultdict
from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Optional, Protocol, TypeVar, Union, cast

from aiokafka import AIOKafkaConsumer
from opentelemetry import trace
from opentelemetry.propagate import extract
from opentelemetry.propagators import textmap
from pydantic import ValidationError

from hexkit.base import InboundProviderBase
from hexkit.correlation import set_correlation_id
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol, DtoValidationError
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import DLQSubscriberProtocol, EventSubscriberProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.utils import (
    generate_client_id,
    generate_ssl_context,
)

CHANGE_EVENT_TYPE = "upserted"
DELETE_EVENT_TYPE = "deleted"
EventOrDaoSubProtocol = Union[DaoSubscriberProtocol, EventSubscriberProtocol]
TextmapHeaders = list[tuple[str, bytes]]

tracer = trace.get_tracer_provider().get_tracer("hexkit.providers.akafka")


class AIOKafkaOtelContextExtractor(textmap.Getter[TextmapHeaders]):
    """OpenTelemetry context extractor.

    This is slighlty adapted from the aiokafka instrumentation library.
    """

    def get(self, carrier: TextmapHeaders, key: str) -> Optional[list[str]]:
        """Extract specific OpenTelemetry header from propagated context"""
        if carrier is None:
            return None

        for item_key, value in carrier:
            if item_key == key and value is not None:
                return [value.decode()]
        return None

    def keys(self, carrier: TextmapHeaders) -> list[str]:
        """Get all headers from propagated context"""
        if carrier is None:
            return []
        return [key for (key, value) in carrier]


class ComboTranslator(EventSubscriberProtocol):
    """Takes a list of translators implementing either the `DaoSubscriberProtocol`,
    `EventSubscriberProtocol`, or both, and create a single translator
    implementing the `EventSubscriberProtocol`. This basically bundles the
    passed-in translators, with the resulting combo translator acting as a router
    that directs the event information to the appropriate translator based on
    its `topic` and `type`.
    """

    topics_of_interest: list[str]
    types_of_interest: list[str]

    def __init__(
        self,
        *,
        translators: Sequence[EventOrDaoSubProtocol],
    ):
        self.translators: dict[str, dict[str, EventOrDaoSubProtocol]] = defaultdict(
            dict
        )
        self.topics_of_interest = []
        self.types_of_interest = []
        uses_outbox = False

        outbox_topics: list[str] = []
        non_outbox_topics: list[str] = []
        for translator in translators:
            if isinstance(translator, DaoSubscriberProtocol):
                outbox_topics.append(translator.event_topic)
                self.translators[translator.event_topic][CHANGE_EVENT_TYPE] = translator
                self.translators[translator.event_topic][DELETE_EVENT_TYPE] = translator
                uses_outbox = True
            elif isinstance(translator, EventSubscriberProtocol):
                # This looks like a looping no-no, but in reality is only a handful of
                #  iterations and it ensures we route the event to the right translator
                self.types_of_interest.extend(translator.types_of_interest)
                non_outbox_topics.extend(dict.fromkeys(translator.topics_of_interest))
                for topic in translator.topics_of_interest:
                    for type_ in translator.types_of_interest:
                        if type_ in self.translators[topic]:
                            raise ValueError(
                                "Got multiple EventSubscriberProtocol-compliant"
                                + " translators trying to consume from the same topic"
                                + " and type."
                            )
                        self.translators[topic][type_] = translator
        if uses_outbox:
            self.types_of_interest.append(CHANGE_EVENT_TYPE)
            self.types_of_interest.append(DELETE_EVENT_TYPE)

        if len(set(outbox_topics)) != len(outbox_topics):
            raise ValueError(
                "Got multiple DaoSubscriberProtocol-compliant translators trying to"
                + " consume from the same event topic."
            )
        self.topics_of_interest.extend(outbox_topics)
        self.topics_of_interest.extend(non_outbox_topics)

    async def _consume_validated(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: Ascii
    ) -> None:
        """
        Receive and process an event with already validated topic, type, and key.

        Args:
            payload: The data/payload to send with the event.
            type_: The type of the event.
            topic: Name of the topic the event was published to.
            key: A key used for routing the event.
        """
        translator = self.translators[topic][type_]

        if translator is None:
            # This should never happen, as the topic should have been filtered out:
            raise RuntimeError

        if isinstance(translator, DaoSubscriberProtocol):
            if type_ == CHANGE_EVENT_TYPE:
                try:
                    dto = translator.dto_model.model_validate(payload)
                except ValidationError as error:
                    message = (
                        f"The event of type {type_} on topic {topic}"
                        + " was not valid wrt. the DTO model."
                    )
                    logging.error(message)
                    raise DtoValidationError(message) from error

                await translator.changed(resource_id=key, update=dto)

            else:
                # a deletion event:
                await translator.deleted(resource_id=key)
        else:
            await translator.consume(payload=payload, type_=type_, topic=topic, key=key)


class HeaderNames:
    """Encapsulated constants for event header values"""

    EVENT_ID = "event_id"
    CORRELATION_ID = "correlation_id"
    ORIGINAL_TOPIC = "original_topic"
    EXC_CLASS = "exc_class"
    EXC_MSG = "exc_msg"


class ConsumerEvent(Protocol):
    """Duck type of an event as received from a KafkaConsumerCompatible."""

    topic: str
    key: str
    value: JsonObject
    headers: list[tuple[str, bytes]]
    partition: int
    offset: int
    timestamp: int


def utc_datetime_from_millis(millis: int) -> datetime:
    """Convert an integer representing milliseconds to a timestamp"""
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)


def now_as_utc() -> datetime:
    """Return the current timestamp with UTC timezone"""
    return datetime.now().astimezone(tz=timezone.utc)


@dataclass
class ExtractedEventInfo:
    """A class encapsulating the data extracted from a `ConsumerEvent`-like object.

    This data includes the topic, type, payload, key, headers, and timestamp of the
    event. If the timestamp is specified in `kwargs`, it will be set to the
    event's timestamp. If the event is not available, it will use the current time.
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

    def asdict(self) -> dict[str, Any]:
        """Return result of `dataclasses.asdict()` on the instance"""
        return asdict(self)


@dataclass
class DLQEventInfo(ExtractedEventInfo):
    """ExtractedEventInfo plus timestamp information"""

    timestamp: datetime

    def __init__(self, event: Optional[ConsumerEvent] = None, **kwargs):
        """Initialize an instance of ExtractedEventInfo."""
        timestamp = utc_datetime_from_millis(event.timestamp) if event else now_as_utc()
        self.timestamp = kwargs.pop("timestamp", timestamp)
        super().__init__(event=event, **kwargs)


def get_event_id(event: ConsumerEvent, *, service_name: str) -> str:
    """Make a label that identifies an event."""
    return f"{service_name},{event.topic},{event.partition},{event.offset}"


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
        translator: Union[EventSubscriberProtocol, DLQSubscriberProtocol],
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
        using_dlq_protocol = isinstance(translator, DLQSubscriberProtocol)

        topics = (
            [config.kafka_dlq_topic]
            if using_dlq_protocol
            else translator.topics_of_interest  # type: ignore[union-attr]
        )

        if config.kafka_enable_dlq:
            if using_dlq_protocol:
                config = config.model_copy(update={"kafka_enable_dlq": False})
                logging.warning(
                    "Can't enable DLQ when using DLQSubscriberProtocol. Disabling DLQ."
                )
            else:
                topics.append("retry-" + config.service_name)
                if dlq_publisher is None:
                    error = ValueError(
                        "A publisher is required when the DLQ is enabled."
                    )
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
        translator: Union[EventSubscriberProtocol, DLQSubscriberProtocol],
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
        self._using_dlq_protocol = isinstance(translator, DLQSubscriberProtocol)
        self._consumer = consumer
        self._translator = translator
        self._types_whitelist = (
            [] if self._using_dlq_protocol else translator.types_of_interest  # type: ignore
        )
        self._dlq_publisher = dlq_publisher
        self._retry_topic = "retry-" + config.service_name
        self._dlq_topic = config.kafka_dlq_topic
        self._service_name = config.service_name
        self._max_retries = config.kafka_max_retries
        self._enable_dlq = config.kafka_enable_dlq
        self._retry_backoff = config.kafka_retry_backoff

    async def _publish_to_dlq(
        self, *, event: ExtractedEventInfo, exc: Exception, event_id: str
    ):
        """Publish the event to the corresponding DLQ topic.

        The exception instance is included in the headers, but is split into the class
        name and the string representation of the exception.

        Args
        - `event`: The event to publish to the DLQ.
        - `exc`: The exception that caused the event to be published to the DLQ.
        - `event_id`: The service name, topic, partition, and offset of the
            failed event. Uses the format "service_name,topic,partition,offset".
        """
        logging.debug("About to publish an event to DLQ topic '%s'", self._dlq_topic)
        await self._dlq_publisher.publish(  # type: ignore
            payload=event.payload,
            type_=event.type_,
            topic=self._dlq_topic,
            key=event.key,
            headers={
                HeaderNames.EXC_CLASS: exc.__class__.__name__,
                HeaderNames.EXC_MSG: str(exc),
                HeaderNames.EVENT_ID: event_id,
                HeaderNames.ORIGINAL_TOPIC: event.topic,
            },
        )
        logging.info("Published event to DLQ topic '%s'", self._dlq_topic)

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
            await self._translator_consume(event=event)
        except Exception as err:
            if retries_left > 0:
                await self._retry_event(event=event, retries_left=retries_left)
            else:
                raise self.RetriesExhaustedError(
                    event_type=event.type_, max_retries=self._max_retries
                ) from err

    async def _translator_consume(self, *, event: ExtractedEventInfo):
        """Pass event to consumer with args adjusted for protocol type."""
        args = asdict(event)
        if not self._using_dlq_protocol:
            # the reason we don't pop 'timestamp' here is because timestamp is only
            # provided if using the DLQ protocol. However, both protocols pass headers
            args.pop("headers")
        await self._translator.consume(**args)

    async def _handle_consumption(self, *, event: ExtractedEventInfo, event_id: str):
        """Try to pass the event to the translator.

        If the event fails:
        1. Retry until retries are exhausted, if retries are configured.
        2. Publish event to DLQ topic if the DLQ is enabled. Done afterward.
           or
        3. Allow failure with unhandled error if the DLQ is not enabled. This is the
           pre-DLQ behavior.

        In the case that the event is published to the DLQ topic, the event ID will be
        added as a header to aid in debugging.
        """
        try:
            await self._translator_consume(event=event)
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
                await self._publish_to_dlq(
                    event=event, exc=underlying_error, event_id=event_id
                )
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
                await self._publish_to_dlq(
                    event=event, exc=underlying_error, event_id=event_id
                )

    def _extract_info(self, event: ConsumerEvent) -> ExtractedEventInfo:
        """Convert the raw event to either ExtractedEventInfo or DLQEventInfo.

        Also extract the original topic name from the header if it's a retried event.
        Automatically extracting the original topic name from retried events prevents
        having to do that separately in every service. The DLQ case is more limited, so
        leave it up to any DLQ topic consumers to extract the value themselves.
        """
        event_info = (
            DLQEventInfo(event)
            if self._using_dlq_protocol
            else ExtractedEventInfo(event)
        )
        if event_info.topic == self._retry_topic:
            # The event is being consumed by from the retry topic, so we expect the
            # original topic to be in the headers.
            event_info.topic = event_info.headers.pop(HeaderNames.ORIGINAL_TOPIC, "")
        return event_info

    def _validate_extracted_info(self, event: ExtractedEventInfo):
        """Validate the extracted event info."""
        correlation_id = event.headers.get(HeaderNames.CORRELATION_ID, "")
        errors = []
        if not event.type_:
            errors.append("event type is empty")
        elif self._types_whitelist and event.type_ not in self._types_whitelist:
            errors.append(f"event type '{event.type_}' is not in the whitelist")
        if not correlation_id:
            errors.append("correlation_id is empty")

        # Check the topic field -- this happens after replacing 'retry' with og topic
        if event.topic == self._retry_topic:
            errors.append(
                f"{HeaderNames.ORIGINAL_TOPIC} header cannot be {self._retry_topic}."
                + f" Value: '{event.topic}'"
            )
        elif not event.topic:
            # only occurs if original_topic header is empty
            errors.append("topic is empty")
        if errors:
            error = RuntimeError(", ".join(errors))
            raise error

    async def _consume_event(self, event: ConsumerEvent) -> None:
        """Consume an event by passing it down to the translator via the protocol."""
        otel_extractor = AIOKafkaOtelContextExtractor()
        extracted_context = extract(
            event.headers,
            getter=otel_extractor,
        )
        with tracer.start_as_current_span(
            name="KafkaEventSubscriber._consume_event", context=extracted_context
        ):
            event_id = get_event_id(event, service_name=self._service_name)
            event_info = self._extract_info(event)
            try:
                self._validate_extracted_info(event_info)
            except RuntimeError as err:
                logging.info(
                    "Ignored event of type '%s': %s, errors: %s",
                    event_info.type_,
                    event_id,
                    str(err),
                )
                # Always acknowledge event receipt for ignored events
                await self._consumer.commit()
                return

            try:
                logging.info(
                    "Consuming event of type '%s': %s", event_info.type_, event_id
                )
                correlation_id = event_info.headers[HeaderNames.CORRELATION_ID]
                async with set_correlation_id(correlation_id):
                    await self._handle_consumption(event=event_info, event_id=event_id)
            except Exception:
                # Errors only bubble up here if the DLQ isn't used
                logging.critical(
                    "An error occurred while processing event of type '%s': %s. It was NOT"
                    " placed in the DLQ topic (%s)",
                    event_info.type_,
                    event_id,
                    self._dlq_topic if self._enable_dlq else "DLQ is disabled",
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

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

import json
import logging

from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord

from hexkit.base import InboundProviderBase
from hexkit.custom_types import JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol


class EventTypeNotFoundError(RuntimeError):
    """Thrown when no `type` was set in the headers of an event."""


def generate_client_id(service_name: str, client_suffix: str) -> str:
    """
    Generate client id (from the perspective of the Kafka broker) by concatenating
    the service name and the client suffix.
    """
    return f"{service_name}.{client_suffix}"


class KafkaEventPublisher(EventPublisherProtocol):
    """Apache Kafka specific event publishing provider."""

    def __init__(
        self,
        *,
        service_name: str,
        client_suffix: str,
        kafka_servers: list[str],
        kafka_producer_cls=KafkaProducer,
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

        self._client_id = generate_client_id(service_name, client_suffix)
        self._kafka_servers = kafka_servers
        self._kafka_producer_cls = kafka_producer_cls
        self._producer: KafkaProducer = None

    def __enter__(self) -> None:
        self._producer = self._kafka_producer_cls(
            bootstrap_servers=self._kafka_servers,
            client_id=self._client_id,
            key_serializer=lambda key: key.encode("ascii"),
            value_serializer=lambda event_value: json.dumps(event_value).encode(
                "ascii"
            ),
        )

    def __exit__(self, *_args) -> None:
        self._producer.close()
        self._producer = None

    def publish(self, *, payload: JsonObject, type_: str, key: str, topic: str) -> None:
        """Publish an event to an Apache Kafka event broker.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        super().publish(payload=payload, type_=type_, key=key, topic=topic)

        event_headers = [("type", type_.encode("ascii"))]
        self._producer.send(topic, key=key, value=payload, headers=event_headers)
        self._producer.flush()


class KafkaEventSubscriber(InboundProviderBase):
    """Apache Kafka-specific event subscription provider."""

    # pylint: disable=too-many-arguments,too-many-instance-attributes
    # (some arguments are only used for testing)
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
        super().__init__()

        self._service_name = service_name
        self._client_id = generate_client_id(service_name, client_suffix)
        self._translator = translator
        self._topics = self._translator.topics_of_interest
        self._types_whitelist = translator.types_of_interest
        self._kakfa_servers = kafka_servers
        self._kafka_consumer_cls = kafka_consumer_cls
        self._consumer: KafkaConsumer = None

    def __enter__(self):
        self._consumer = self._kafka_consumer_cls(
            *self._topics,
            bootstrap_servers=self._kakfa_servers,
            client_id=self._client_id,
            group_id=self._service_name,
            auto_offset_reset="earliest",
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
        )

    def __exit__(self, *_args):
        self._consumer.close()
        self._consumer = None

    @staticmethod
    def _get_type(event: ConsumerRecord) -> str:
        """Extract the event type out of an ConsumerRecord."""
        for header in event.headers:
            if header[0] == "type":
                return header[1].decode("ascii")
        raise EventTypeNotFoundError()

    @staticmethod
    def _get_event_label(event: ConsumerRecord) -> str:
        """Get a label that identifies an event."""
        return (
            f"{event.topic} - {event.partition} - {event.offset} "
            + " (topic-partition-offset)"
        )

    def _consume_event(self, event: ConsumerRecord) -> None:
        """Consume an event by passing it down to the translator via the protocol."""
        event_label = self._get_event_label(event)

        try:
            type_ = self._get_type(event)
        except EventTypeNotFoundError:
            logging.warning("Ignored an event without type: %s", event_label)
        else:

            if type_ in self._types_whitelist:
                logging.info('Consuming event of type "%s": %s', type_, event_label)

                try:
                    # blocks until event processing is completed:
                    self._translator.consume(
                        payload=event.value, type_=type_, topic=event.topic
                    )
                except Exception:
                    logging.error(
                        "A fatal error occured while processing the event: %s",
                        event_label,
                    )
                    raise

            else:
                logging.info("Ignored event of type %s: %s", type_, event_label)

    def run(self, forever: bool = True) -> None:
        """
        Start consuming events and passing them down to the translator.
        By default, it blocks forever.
        However, you can set `forever` to `False` to make it return after handling one
        event.
        """
        super().run(forever=forever)

        if forever:
            for event in self._consumer:
                self._consume_event(event)
        else:
            event = next(self._consumer)
            self._consume_event(event)

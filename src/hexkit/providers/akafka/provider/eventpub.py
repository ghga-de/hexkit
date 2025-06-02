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
Apache Kafka-specific event publisher provider implementing the
`EventPublisherProtocol`.
"""

import json
import logging
import ssl
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional, Protocol
from uuid import UUID

from aiokafka import AIOKafkaProducer

from hexkit.correlation import (
    CorrelationIdContextError,
    get_correlation_id,
    new_correlation_id,
    validate_correlation_id,
)
from hexkit.custom_types import Ascii, JsonObject, KafkaCompressionType
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.utils import (
    generate_client_id,
    generate_ssl_context,
)

RESERVED_HEADERS = ["type", "correlation_id"]


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
        max_request_size: int,
        compression_type: Optional[KafkaCompressionType] = None,
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
            max_request_size:
                Maximum sendable message size.
            compression_type:
                Compression type to use for messages. Defaults to None.
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
            value_serializer=cls._default_event_value_serializer,
            max_request_size=config.kafka_max_message_size,
            compression_type=config.kafka_compression_type,
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

    @classmethod
    def _default_event_value_serializer(cls, value: JsonObject) -> bytes:
        """Method used as default event value serializer."""
        return json.dumps(value, default=cls._default_json_serializer).encode("ascii")

    @classmethod
    def _default_json_serializer(cls, value: Any) -> str:
        """Method used as default JSON serializer for events."""
        if isinstance(value, (UUID, Path)):
            return str(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value)} is not JSON serializable")

    async def _publish_validated(
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        key: Ascii,
        topic: Ascii,
        headers: Mapping[str, str],
    ) -> None:
        """Publish an event with already validated topic and type.

        Args:
        - `payload` (JSON): The payload to ship with the event.
        - `type_` (str): The event type. ASCII characters only.
        - `key` (str): The event key. ASCII characters only.
        - `topic` (str): The event topic. ASCII characters only.
        - `headers`: Additional headers to attach to the event.
        """
        try:
            correlation_id = get_correlation_id()
        except CorrelationIdContextError:
            if not self._generate_correlation_id:
                raise

            correlation_id = new_correlation_id()
            logging.info("Generated new correlation ID: %s", correlation_id)

        validate_correlation_id(correlation_id)

        # Create a shallow copy of the headers
        headers_copy = dict(headers)

        # Check and log warnings for reserved headers
        for header in RESERVED_HEADERS:
            log_msg = (
                f"The '{header}' header shouldn't be supplied, but was. Overwriting."
            )
            if header in headers_copy:
                logging.warning(log_msg, extra={header: headers_copy[header]})

        headers_copy["type"] = type_
        headers_copy["correlation_id"] = correlation_id
        encoded_headers_list = [(k, v.encode("ascii")) for k, v in headers_copy.items()]

        await self._producer.send_and_wait(
            topic, key=key, value=payload, headers=encoded_headers_list
        )

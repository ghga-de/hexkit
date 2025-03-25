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

"""Apache Kafka-specific provider using implementations of the
`DaoSubscriberProtocol`.
"""

import logging
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import Optional

from aiokafka import AIOKafkaConsumer

from hexkit.base import InboundProviderBase
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.eventsub import (
    CHANGE_EVENT_TYPE,  # noqa: F401  (export for backwards compatibility until v5)
    DELETE_EVENT_TYPE,  # noqa: F401  (export for backwards compatibility until v5)
    KafkaConsumerCompatible,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.provider.eventsub import (
    ComboTranslator as TranslatorConverter,  # for backwards compatibility
)


class KafkaOutboxSubscriber(InboundProviderBase):
    """Apache Kafka-specific provider using translators that implement the
    `DaoSubscriberProtocol`.

    Note: this class can be replaced by using the ComboTranslator in conjunction
    with the normal KafkaEventSubscriber class. We may therefore deprecate this
    class in the near future.
    """

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        translators: Sequence[DaoSubscriberProtocol],
        dlq_publisher: Optional[EventPublisherProtocol] = None,
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
    ):
        """Setup and teardown an instance of the provider.

        Args:
        - `config`: MongoDB-specific config parameters.
        - `translators`: A sequence of translators implementing the
            `DaoSubscriberProtocol`.
        - `dlq_publisher`: An instance of the publisher to use for the DLQ. Can be None
            if not using the dead letter queue. It is used to publish events to the DLQ.
        - `kafka_consumer_cls`: The Kafka consumer class to use. Defaults to
            `AIOKafkaConsumer`.

        Returns:
            An instance of the provider.
        """
        translator_converter = TranslatorConverter(translators=translators)

        if config.kafka_enable_dlq and dlq_publisher is None:
            error = ValueError("A publisher is required when the DLQ is enabled.")
            logging.error(error)
            raise error

        async with KafkaEventSubscriber.construct(
            config=config,
            translator=translator_converter,
            dlq_publisher=dlq_publisher,
            kafka_consumer_cls=kafka_consumer_cls,
        ) as event_subscriber:
            yield cls(event_subscriber=event_subscriber)

    def __init__(self, *, event_subscriber: KafkaEventSubscriber):
        """Please do not call directly! Should be called by the `construct` method."""
        self._event_subscriber = event_subscriber

    async def run(self, forever: bool = True) -> None:
        """Start consuming events from the Kafka outbox.

        Args:
            forever: Whether to run the consumer indefinitely. Defaults to True.
        """
        await self._event_subscriber.run(forever=forever)

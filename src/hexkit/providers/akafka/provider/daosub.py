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

"""Apache Kafka-specific provider using implementations of the
`DaoSubscriberProtocol`.
"""

import logging
from collections.abc import Sequence
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from hexkit.base import InboundProviderBase
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import (
    DaoSubscriberProtocol,
    DtoValidationError,
)
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.provider.eventsub import (
    KafkaConsumerCompatible,
    KafkaEventSubscriber,
)

CHANGE_EVENT_TYPE = "upserted"
DELETE_EVENT_TYPE = "deleted"


class TranslatorConverter(EventSubscriberProtocol):
    """Takes a list of translators implementing the `DaoSubscriberProtocol` to
    create a single translator implementing the `EventSubscriberProtocol`.
    """

    types_of_interest = [CHANGE_EVENT_TYPE, DELETE_EVENT_TYPE]

    def __init__(self, *, translators: Sequence[DaoSubscriberProtocol]):
        self.topics_of_interest = [translator.event_topic for translator in translators]

        if len(set(self.topics_of_interest)) != len(self.topics_of_interest):
            raise ValueError(
                "Got multiple DaoSubscriberProtocol-compliant translators trying to"
                + " consume from the same event topic."
            )

        self._translator_by_topic = {
            translator.event_topic: translator for translator in translators
        }

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
        translator = self._translator_by_topic.get(topic)

        if translator is None:
            # This should never happen, as the topic should have been filtered out:
            raise RuntimeError

        if type_ == CHANGE_EVENT_TYPE:
            try:
                dto = translator.dto_model.model_validate(payload)
            except ValidationError as error:
                message = (
                    f"The event of type {type_} on topic {topic} was not valid wrt. the"
                    + " DTO model."
                )
                logging.error(message)
                raise DtoValidationError(message) from error

            await translator.changed(resource_id=key, update=dto)

        else:
            # a deletion event:
            await translator.deleted(resource_id=key)


class KafkaOutboxSubscriber(InboundProviderBase):
    """Apache Kafka-specific provider using translators that implement the
    `DaoSubscriberProtocol`.
    """

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: KafkaConfig,
        translators: Sequence[DaoSubscriberProtocol],
        kafka_consumer_cls: type[KafkaConsumerCompatible] = AIOKafkaConsumer,
    ):
        """Setup and teardown an instance of the provider.

        Args:
            config: MongoDB-specific config parameters.

        Returns:
            An instance of the provider.
        """
        translator_converter = TranslatorConverter(translators=translators)

        async with KafkaEventSubscriber.construct(
            config=config,
            translator=translator_converter,
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

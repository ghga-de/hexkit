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
"""A Kafka event publisher featuring a DAO that allows for storing stateless events
in the database. This functionality is the event-publishing-focused counterpart of the
`MongoKafkaDaoPublisher`.

Requires dependencies of the `akafka` and `mongodb` extras.
"""

from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import uuid4

from aiokafka import AIOKafkaProducer
from pydantic import UUID4, BaseModel, Field

from hexkit.correlation import (
    CorrelationIdContextError,
    get_correlation_id,
    new_correlation_id,
    set_correlation_id,
)
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.dao import Dao, UUID4Field
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.providers.akafka.provider.eventpub import (
    KafkaEventPublisher,
    KafkaProducerCompatible,
)
from hexkit.providers.mongodb.provider import (
    MongoDbDaoFactory,
    translate_pymongo_errors,
)
from hexkit.providers.mongokafka.provider.config import MongoKafkaConfig
from hexkit.utils import now_utc_ms_prec


class PersistentKafkaEvent(BaseModel):
    """A model representing a kafka event to be published and stored in the database."""

    compaction_key: str = Field(
        ...,
        description="The unique ID of the event. If the topic is set to be compacted,"
        + " the ID is set to the topic and key in the format <topic>:<key>. Otherwise"
        + " the ID is set to the actual event ID.",
    )
    topic: Ascii = Field(..., description="The event topic")
    payload: JsonObject = Field(..., description="The event payload")
    key: Ascii = Field(..., description="The event key")
    type_: Ascii = Field(..., description="The event type")
    event_id: UUID4 | None = Field(default=None, description="The event ID")
    headers: Mapping[str, str] = Field(
        default_factory=dict,
        description="Non-standard event headers. Correlation ID, event_id, and event"
        + " type are transmitted as event headers, but added as such within the publisher"
        + " protocol. The headers here are any additional header that need to be sent.",
    )
    correlation_id: UUID4 = UUID4Field(description="The event correlation ID")
    created: datetime = Field(
        ..., description="The timestamp of when the event record was first inserted"
    )
    published: bool = Field(False, description="Whether the event has been published")


class PersistentKafkaPublisher(EventPublisherProtocol):
    """A Kafka event publisher that uses a MongoDB DAO to store stateless events as-is.

    This class should be used for events that do not represent a stateful object,
    such as user info, but rather stateless information. This includes things like
    notifications, completed actions, file processing results, etc.
    """

    @classmethod
    @asynccontextmanager
    async def construct(  # noqa: PLR0913
        cls,
        *,
        config: MongoKafkaConfig,
        dao_factory: MongoDbDaoFactory,
        compacted_topics: set[str] | None = None,
        topics_not_stored: set[str] | None = None,
        collection_name: str = "",
        kafka_producer_cls: type[KafkaProducerCompatible] = AIOKafkaProducer,
    ):
        """
        Setup and teardown KafkaEventPublisher instance with some config params.

        Args:
            config:
                Config parameters needed for connecting to Apache Kafka.
            compacted_topics:
                A set of topics that should be compacted. For these topics, only the
                latest event for a given key will be republished. Prior events for
                a given key in a compacted topic are replaced by new events, so only
                one event should be stored at a given time per key per topic.
            topics_not_stored:
                A set of topics which should not be stored in the database. Events
                for these topics will be published identically as they would be in
                the `KafkaEventPublisher` class.
            collection_name:
                The name of the MongoDB collection in which to store events.
            dao_factory:
                A MongoDbDaoFactory instance that can be used to create a DAO.
            kafka_producer_cls:
                Overwrite the used Kafka Producer class. Only intended for unit testing.
        """
        compacted_topics = compacted_topics or set()
        topics_not_stored = topics_not_stored or set()

        conflicts = topics_not_stored.intersection(compacted_topics)
        if conflicts:
            conflict_list = sorted(conflicts)
            raise ValueError(
                "Values for `topics_not_stored` and `compacted_topics` must be exclusive."
                + f" Please review the following values: {', '.join(conflict_list)}."
            )

        collection_name = collection_name or f"{config.service_name}PersistedEvents"
        dao = await dao_factory.get_dao(
            name=collection_name,
            id_field="compaction_key",
            dto_model=PersistentKafkaEvent,
        )
        async with KafkaEventPublisher.construct(
            config=config,
            kafka_producer_cls=kafka_producer_cls,
        ) as event_publisher:
            yield cls(
                event_publisher=event_publisher,
                dao=dao,
                compacted_topics=compacted_topics,
                topics_not_stored=topics_not_stored,
            )

    def __init__(
        self,
        *,
        event_publisher: KafkaEventPublisher,
        dao: Dao[PersistentKafkaEvent],
        compacted_topics: set[str],
        topics_not_stored: set[str],
    ):
        """Please do not call directly! Should be called by the `construct` method."""
        self._event_publisher = event_publisher
        self._dao = dao
        self._compacted_topics = compacted_topics
        self._topics_not_stored = topics_not_stored

    async def _publish_validated(  # noqa: PLR0913
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        key: Ascii,
        topic: Ascii,
        event_id: UUID4,
        headers: Mapping[str, str],
    ) -> None:
        """Publish an event with already validated topic and type.

        Args:
        - `payload` (JSON): The payload to ship with the event.
        - `type_` (str): The event type. ASCII characters only.
        - `key` (str): The event key. ASCII characters only.
        - `topic` (str): The event topic. ASCII characters only.
        - `event_id` (UUID): The event ID.
        - `headers`: Additional headers to attach to the event.
        """
        # For topics that aren't meant to be stored, do normal publish and return
        if topic in self._topics_not_stored:
            await self._event_publisher.publish(
                payload=payload,
                topic=topic,
                type_=type_,
                key=key,
                event_id=event_id,
                headers=headers,
            )
            return

        # Otherwise, perform logic to upsert, publish, and update the event
        try:
            correlation_id = get_correlation_id()
        except CorrelationIdContextError:
            correlation_id = new_correlation_id()

        created = now_utc_ms_prec()

        # Create an compaction key containing the topic and key for compacted topics.
        # If the topic is not compacted, just use the event ID.
        compaction_key = (
            f"{topic}:{key}" if topic in self._compacted_topics else str(event_id)
        )

        # Create an instance of the pydantic model representing the event
        event = PersistentKafkaEvent(
            compaction_key=compaction_key,
            topic=topic,
            type_=type_,
            key=key,
            payload=payload,
            headers=headers,
            correlation_id=correlation_id,
            event_id=event_id,
            created=created,
            published=False,
        )

        # Upsert the event initially as 'unpublished' before publishing. Upsertion
        #  does the work to mimic topic compaction on selected topics due to the fixed
        #  key. On non-compacted topics, there is no effect as the ID is a random UUID.
        await self._dao.upsert(event)
        await self._publish_and_update(event)

    async def _publish_and_update(self, event: PersistentKafkaEvent) -> None:
        """Publishes an event and marks it as 'published' in the database."""
        async with set_correlation_id(event.correlation_id):
            await self._event_publisher.publish(
                topic=event.topic,
                type_=event.type_,
                key=event.key,
                payload=event.payload,
                event_id=event.event_id,
                headers=event.headers,
            )

        # Update the event to be marked as 'published' if it hasn't been already
        if not event.published:
            event.published = True
            await self._dao.update(event)

    async def publish_pending(self) -> None:
        """Publishes all non-published events."""
        with translate_pymongo_errors():
            events = [
                dto async for dto in self._dao.find_all(mapping={"published": False})
            ]

        events.sort(key=lambda x: x.created)

        for event in events:
            # If there's no event ID, generate a new UUID.
            if not event.event_id:
                event.event_id = uuid4()
            await self._publish_and_update(event)

    async def republish(self) -> None:
        """Republishes all stored events independent of whether they have
        already been published or not.
        """
        with translate_pymongo_errors():
            events = self._dao.find_all(mapping={})

        async for event in events:
            # If there's no event ID, generate a new UUID. It will get stored when the
            # event is published. Set `published` to `False` to trigger an update.
            if not event.event_id:
                event.event_id = uuid4()
                event.published = False
            await self._publish_and_update(event)

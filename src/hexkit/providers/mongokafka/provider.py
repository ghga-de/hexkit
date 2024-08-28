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

"""An implementation of the DaoPublisherFactoryProtocol based on MongoDB and Apache Kafka.

Require dependencies of the `akafka` and `mongodb` extras.
"""

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Collection, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from typing import Any, Callable, Generic, Optional

from aiokafka import AIOKafkaProducer
from motor.core import AgnosticCollection
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import field_validator

from hexkit.correlation import get_correlation_id, set_correlation_id
from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import (
    DaoNaturalId,
    Dto,
    ResourceNotFoundError,
)
from hexkit.protocols.daopub import DaoPublisher, DaoPublisherFactoryProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.providers.akafka import KafkaConfig, KafkaEventPublisher
from hexkit.providers.akafka.provider.daosub import CHANGE_EVENT_TYPE, DELETE_EVENT_TYPE
from hexkit.providers.akafka.provider.eventpub import KafkaProducerCompatible
from hexkit.providers.mongodb.provider import (
    MongoDbConfig,
    MongoDbDaoNaturalId,
    get_single_hit,
    replace_id_field_in_find_mapping,
    validate_find_mapping,
)


class ResourceDeletedError(RuntimeError):
    """Raised when trying to interact with a resource that has been deleted."""

    def __init__(self, id_: str):
        """Initialize the exception."""
        super().__init__(f"Resource with ID {id_} has been deleted.")
        self.id_ = id_


def document_to_dto(
    document: dict[str, Any], *, id_field: str, dto_model: type[Dto]
) -> Dto:
    """Converts a document obtained from the MongoDB database into a DTO model-
    compliant representation.

    Raises:
        ResourceDeletedError:
            If the documents `__metadata__` field indicates that the resource has
            been deleted.
    """
    if document.get("__metadata__", {}).get("deleted", False):
        raise ResourceDeletedError(id_=document["_id"])

    document_cleaned = document.copy()
    _ = document_cleaned.pop("__metadata__", None)
    document_cleaned[id_field] = document_cleaned.pop("_id")

    return dto_model.model_validate(document_cleaned)


def dto_to_document(
    dto: Dto, *, id_field: str, published: bool = False
) -> dict[str, Any]:
    """Converts a DTO into a representation that is a compatible document for a
    MongoDB Database.
    """
    document = json.loads(dto.model_dump_json())
    document["_id"] = document.pop(id_field)

    correlation_id = get_correlation_id()
    document["__metadata__"] = {
        "deleted": False,
        "published": published,
        "correlation_id": correlation_id,
    }

    return document


def get_change_publish_func(
    id_field: str,
    event_topic: str,
    dto_to_event: Callable[[Dto], Optional[JsonObject]],
    event_publisher: EventPublisherProtocol,
    collection: AgnosticCollection,
) -> Callable[[Dto], Awaitable[None]]:
    """Generate a function that publishes change events for a specific type of resource."""

    async def publish_change(dto: Dto) -> None:
        """Publishes a change event and marks the change as published."""
        payload = dto_to_event(dto)
        if payload is not None:
            await event_publisher.publish(
                payload=payload,
                type_=CHANGE_EVENT_TYPE,
                key=getattr(dto, id_field),
                topic=event_topic,
            )

        document = dto_to_document(dto, id_field=id_field, published=True)
        await collection.replace_one({"_id": document["_id"]}, document, upsert=True)

    return publish_change


def get_delete_publish_func(
    event_topic: str,
    event_publisher: EventPublisherProtocol,
    collection: AgnosticCollection,
) -> Callable[[str], Awaitable[None]]:
    """Generate a function that publishes deletion events for a specific type of
    resource.
    """

    async def publish_deletion(id_: str) -> None:
        """Publishes a deletion event and marks the deletion as published."""
        await event_publisher.publish(
            payload={},
            type_=DELETE_EVENT_TYPE,
            key=id_,
            topic=event_topic,
        )

        correlation_id = get_correlation_id()  # Get active correlation first
        document = {
            "_id": id_,
            "__metadata__": {
                "deleted": True,
                "published": True,
                "correlation_id": correlation_id,
            },
        }
        await collection.replace_one({"_id": document["_id"]}, document)

    return publish_deletion


@contextmanager
def assert_not_deleted():
    """A context manager that translates ResourceDeletedError into ResourceNotFoundError."""
    try:
        yield
    except ResourceDeletedError as error:
        raise ResourceNotFoundError(id_=error.id_) from error


class MongoKafkaDaoPublisher(Generic[Dto]):
    """A DAO that uses a natural resource ID provided by the client."""

    @classmethod
    def with_transaction(cls) -> AbstractAsyncContextManager["DaoNaturalId[Dto]"]:
        """Creates a transaction manager that uses an async context manager interface:

        Upon __aenter__, pens a new transactional scope. Returns a transaction-scoped
        DAO.

        Upon __aexit__, closes the transactional scope. A full rollback of the
        transaction is performed in case of an exception. Otherwise, the changes to the
        database are committed and flushed.
        """
        raise NotImplementedError

    def __init__(  # noqa: PLR0913
        self,
        *,
        id_field: str,
        dto_model: type[Dto],
        collection: AgnosticCollection,
        dao: MongoDbDaoNaturalId[Dto],
        publish_change: Callable[[Dto], Awaitable[None]],
        publish_delete: Callable[[str], Awaitable[None]],
        autopublish: bool,
    ):
        """Initialize the DAO.

        Args:
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            collection:
                A collection object from the motor library.
            dao:
                The actual DAO implementation that provides the database-specific
                functionality.
            publish_change:
                A callable returning an awaitable for publishing the current state of a
                created or changed resource.
            publish_delete:
                A callable returning an awaitable for publishing the deletion of a
                resource.
            autopublish:
                Whether to automatically publish changes.
        """
        self._id_field = id_field
        self._dto_model = dto_model
        self._collection = collection
        self._dao = dao
        self._publish_change = publish_change
        self._publish_delete = publish_delete
        self._autopublish = autopublish

    async def get_by_id(self, id_: str) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        with assert_not_deleted():
            return await self._dao.get_by_id(id_)

    async def update(self, dto: Dto) -> None:
        """Update an existing resource.

        Args:
            dto:
                The updated resource content as a pydantic-based data transfer object
                including the resource ID.

        Raises:
            ResourceNotFoundError:
                when resource with the id specified in the dto was not found
        """
        correlation_id = get_correlation_id()
        document = self._dao._dto_to_document(dto)
        document.setdefault("__metadata__", {})["correlation_id"] = correlation_id
        result = await self._collection.replace_one(
            {
                "_id": document["_id"],
                "$or": [
                    {"__metadata__": {"$exists": False}},
                    {"__metadata__.deleted": False},
                ],
            },
            document,
        )
        if result.matched_count == 0:
            raise ResourceNotFoundError(id_=document["_id"])

        if self._autopublish:
            await self._publish_change(dto)

    async def delete(self, id_: str) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        correlation_id = get_correlation_id()
        document = {
            "_id": id_,
            "__metadata__": {
                "deleted": True,
                "published": False,
                "correlation_id": correlation_id,
            },
        }
        result = await self._collection.replace_one(
            {
                "_id": document["_id"],
                "$or": [
                    {"__metadata__": {"$exists": False}},
                    {"__metadata__.deleted": False},
                ],
            },
            document,
        )
        if result.matched_count == 0:
            raise ResourceNotFoundError(id_=id_)

        if self._autopublish:
            await self._publish_delete(id_)

    async def find_one(self, *, mapping: Mapping[str, Any]) -> Dto:
        """Find the resource that matches the specified mapping. It is expected that
        at most one resource matches the constraints. An exception is raised if no or
        multiple hits are found.

        Args:
            mapping:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields

        Returns:
            Returns a hit in the form of the respective DTO model if exactly one hit
            was found that matches the given mapping.

        Raises:
            NoHitsFoundError:
                If no hit was found.
            MultipleHitsFoundError:
                Raised when obtaining more than one hit.
        """
        hits = self.find_all(mapping=mapping)
        return await get_single_hit(hits=hits, mapping=mapping)

    async def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[Dto]:
        """Find all resources that match the specified mapping.

        Args:
            mapping:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields.

        Returns:
            An AsyncIterator of hits. All hits are in the form of the respective DTO
            model.
        """
        validate_find_mapping(mapping, dto_model=self._dto_model)
        mapping = replace_id_field_in_find_mapping(mapping, self._id_field)

        cursor = self._collection.find(filter=mapping)

        async for document in cursor:
            if document.get("__metadata__", {}).get("deleted", False):
                continue
            yield document_to_dto(
                document, id_field=self._id_field, dto_model=self._dto_model
            )

    async def insert(self, dto: Dto) -> None:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.

        Raises:
            ResourceAlreadyExistsError:
                when a resource with the ID specified in the dto does already exist.
        """
        await self._dao.insert(dto=dto)

        if self._autopublish:
            await self._publish_change(dto)

    async def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """
        await self._dao.upsert(dto=dto)
        if self._autopublish:
            await self._publish_change(dto)

    async def publish_document(self, document: dict[str, Any]) -> None:
        """Publishes a document"""
        correlation_id = document.get("__metadata__", {}).get("correlation_id", "")
        async with set_correlation_id(correlation_id=correlation_id):
            if document.get("__metadata__", {}).get("deleted", False):
                await self._publish_delete(document["_id"])
            else:
                dto = document_to_dto(
                    document, id_field=self._id_field, dto_model=self._dto_model
                )
                await self._publish_change(dto)

    async def publish_pending(self) -> None:
        """Publishes all non-published changes."""
        cursor = self._collection.find(filter={"__metadata__.published": False})

        async for document in cursor:
            await self.publish_document(document)

    async def republish(self) -> None:
        """Republishes the state of all resources independent of whether they have
        already been published or not.
        """
        cursor = self._collection.find()

        async for document in cursor:
            await self.publish_document(document)


class MongoKafkaConfig(MongoDbConfig, KafkaConfig):
    """Config parameters and their defaults."""

    @field_validator("kafka_max_message_size", mode="after")
    @classmethod
    def validate_max_message_size(cls, value: int) -> int:
        """Validate the maximum message size."""
        if value > 2**24:  # 16 MiB
            logging.warning(
                f"Max message size ({value}) exceeds the 16 MiB document size limit for MongoDB!"
            )
        return value


class MongoKafkaDaoPublisherFactory(DaoPublisherFactoryProtocol):
    """A provider implementing the DaoPublisherFactoryProtocol based on MongoDB and
    Apache Kafka.
    """

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: MongoKafkaConfig,
        kafka_producer_cls: type[KafkaProducerCompatible] = AIOKafkaProducer,
    ):
        """Setup and teardown an instance of the provider.

        Args:
            config: MongoDB-specific config parameters.

        Returns:
            An instance of the provider.
        """
        async with KafkaEventPublisher.construct(
            config=config, kafka_producer_cls=kafka_producer_cls
        ) as event_publisher:
            yield cls(config=config, event_publisher=event_publisher)

    def __init__(
        self, *, config: MongoKafkaConfig, event_publisher: EventPublisherProtocol
    ):
        """Please do not call directly! Should be called by the `construct` method."""
        self._config = config

        # get a database-specific client:
        self._client: AsyncIOMotorClient = AsyncIOMotorClient(
            self._config.db_connection_str.get_secret_value()
        )
        self._db = self._client[self._config.db_name]

        self._event_publisher = event_publisher

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    async def _get_dao(  # noqa: PLR0913
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]],
        dto_to_event: Callable[[Dto], Optional[JsonObject]],
        event_topic: str,
        autopublish: bool,
    ) -> DaoPublisher[Dto]:
        """Constructs a DAO for interacting with resources in a MongoDB database.
        Updates are automatically published to Apache Kafka.

        Please see the DaoPublisherFactoryProtocol superclass for documentation of
        parameters.
        """
        if fields_to_index is not None:
            raise NotImplementedError(
                "Indexing on non-ID fields has not been implemented, yet."
            )

        collection = self._db[name]

        document_to_dto_ = lambda document: document_to_dto(
            document=document, id_field=id_field, dto_model=dto_model
        )
        dto_to_document_ = lambda dto: dto_to_document(dto=dto, id_field=id_field)

        dao = MongoDbDaoNaturalId(
            collection=collection,
            dto_model=dto_model,
            id_field=id_field,
            document_to_dto=document_to_dto_,
            dto_to_document=dto_to_document_,
        )

        publish_change = get_change_publish_func(
            id_field=id_field,
            event_topic=event_topic,
            dto_to_event=dto_to_event,
            event_publisher=self._event_publisher,
            collection=collection,
        )
        publish_delete = get_delete_publish_func(
            event_topic=event_topic,
            event_publisher=self._event_publisher,
            collection=collection,
        )

        return MongoKafkaDaoPublisher(
            id_field=id_field,
            dto_model=dto_model,
            collection=collection,
            dao=dao,
            publish_change=publish_change,
            publish_delete=publish_delete,
            autopublish=autopublish,
        )

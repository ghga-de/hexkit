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

"""MongoDB-based provider implementing the DaoFactoryProtocol.

Utilities for testing are located in `./testutils.py`.
"""

# ruff: noqa: PLR0913

import json
from abc import ABC
from collections.abc import AsyncGenerator, AsyncIterator, Collection, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, Callable, Generic, Optional, Union, overload

from motor.core import AgnosticCollection
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings
from pymongo.errors import DuplicateKeyError

from hexkit.protocols.dao import (
    DaoFactoryProtocol,
    DaoNaturalId,
    DaoSurrogateId,
    Dto,
    DtoCreation,
    DtoCreation_contra,
    InvalidFindMappingError,
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = ["MongoDbConfig", "MongoDbDaoFactory"]


def document_to_dto(
    document: dict[str, Any], *, id_field: str, dto_model: type[Dto]
) -> Dto:
    """Converts a document obtained from the MongoDB database into a DTO model-
    compliant representation.
    """
    document[id_field] = document.pop("_id")
    return dto_model.model_validate(document)


def dto_to_document(dto: Dto, *, id_field: str) -> dict[str, Any]:
    """Converts a DTO into a representation that is a compatible document for a
    MongoDB Database.
    """
    document = json.loads(dto.model_dump_json())
    document["_id"] = document.pop(id_field)

    return document


def validate_find_mapping(mapping: Mapping[str, Any], *, dto_model: type[Dto]):
    """Validates a key/value mapping used in find methods against the provided DTO model by checking if the mapping keys exist as model fields.

    Raises:
        InvalidMappingError: If validation fails.
    """
    try:
        validate_fields_in_model(model=dto_model, fields=set(mapping))
    except FieldNotInModelError as error:
        raise InvalidFindMappingError(
            f"The provided find mapping was invalid: {error}."
        ) from error


def replace_id_field_in_find_mapping(
    mapping: Mapping[str, Any], id_field: str
) -> Mapping[str, Any]:
    """If the provided find mapping includes the ID field, it is replaced with MongoDB's
    internal ID field name.
    """
    if id_field in mapping:
        mapping = dict(mapping)
        mapping["_id"] = mapping.pop(id_field)

    return mapping


async def get_single_hit(
    *, hits: AsyncIterator[Dto], mapping: Mapping[str, Any]
) -> Dto:
    """Asserts that there is exactly one hit in the provided AsyncIterator of hits and
    returns it.

    Args:
        hits: An AsyncIterator of hits resulting from a find operation.
        mapping: The mapping that was used to obtain the hits.

    Raises:
        NoHitsFoundError: If no hit was found.
        MultipleHitsFoundError: If more than one hit was found.
    """
    try:
        dto = await hits.__anext__()
    except StopAsyncIteration as error:
        raise NoHitsFoundError(mapping=mapping) from error

    try:
        _ = await hits.__anext__()
    except StopAsyncIteration:
        # This is expected:
        return dto

    raise MultipleHitsFoundError(mapping=mapping)


class MongoDbDaoBase(ABC, Generic[Dto]):
    """A base class with methods common to all MongoDB-based DAOs.
    This shall be used as base class for other MongoDB-based DAO implementations.
    """

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        id_field: str,
        collection: AgnosticCollection,
        document_to_dto: Callable[[dict[str, Any]], Dto],
        dto_to_document: Callable[[Dto], dict[str, Any]],
    ):
        """Initialize the DAO.

        Args:
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            collection:
                A collection object from the motor library.
            document_to_dto:
                A callable that takes a document obtained from the MongoDB database
                and returns a DTO model-compliant representation.
            dto_to_document:
                A callable that takes a DTO model and returns a representation that is
                a compatible document for a MongoDB database.
        """
        self._collection = collection
        self._dto_model = dto_model
        self._id_field = id_field
        self._document_to_dto = document_to_dto
        self._dto_to_document = dto_to_document

    async def get_by_id(self, id_: str) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        document = await self._collection.find_one({"_id": id_})

        if document is None:
            raise ResourceNotFoundError(id_=id_)

        return self._document_to_dto(document)

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
        document = self._dto_to_document(dto)
        result = await self._collection.replace_one({"_id": document["_id"]}, document)

        if result.matched_count == 0:
            raise ResourceNotFoundError(id_=document["_id"])

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

    async def delete(self, id_: str) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        result = await self._collection.delete_one({"_id": id_})

        if result.deleted_count == 0:
            raise ResourceNotFoundError(id_=id_)

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

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
            yield self._document_to_dto(document)


class MongoDbDaoSurrogateId(MongoDbDaoBase[Dto], Generic[Dto, DtoCreation_contra]):
    """A DAO that generates an internal/surrogate key for
    identifying resources in the database. ID/keys cannot be defined by the client of
    the DAO. Thus, both a standard DTO model (first type parameter), which includes
    the key field, as well as special DTO model (second type parameter), which is
    identical to the first one, but does not include the ID field and is dedicated for
     creation of new resources, is needed.
    """

    @classmethod
    def with_transaction(
        cls,
    ) -> AbstractAsyncContextManager["DaoSurrogateId[Dto, DtoCreation_contra]"]:
        """Creates a transaction manager that uses an async context manager interface:

        Upon __aenter__, pens a new transactional scope. Returns a transaction-scoped
        DAO.

        Upon __aexit__, closes the transactional scope. A full rollback of the
        transaction is performed in case of an exception. Otherwise, the changes to the
        database are committed and flushed.
        """
        raise NotImplementedError()

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        dto_creation_model: type[DtoCreation_contra],
        id_field: str,
        collection: AgnosticCollection,
        id_generator: AsyncGenerator[str, None],
        document_to_dto: Callable[[dict[str, Any]], Dto],
        dto_to_document: Callable[[Dto], dict[str, Any]],
    ):
        """Initialize the DAO.

        Args:
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            dto_creation_model:
                A DTO model specific for creation of a new resource. This
                model has to be identical to the `dto_model` except that it has to miss
                the `id_field`. The resource ID will be generated by the DAO
                implementation upon resource creation.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            collection:
                A collection object from the motor library.
            id_generator:
                A generator to continuously generate new IDs for new resources.
            document_to_dto:
                A callable that takes a document obtained from the MongoDB database
                and returns a DTO model-compliant representation.
            dto_to_document:
                A callable that takes a DTO model and returns a representation that is
                a compatible document for a MongoDB database.
        """
        super().__init__(
            dto_model=dto_model,
            id_field=id_field,
            collection=collection,
            document_to_dto=document_to_dto,
            dto_to_document=dto_to_document,
        )

        self._dto_creation_model = dto_creation_model
        self._id_generator = id_generator

    async def insert(self, dto: DtoCreation_contra) -> Dto:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object without the
                resource ID (which will be set automatically).

        Returns:
            Returns a copy of the newly inserted resource including its assigned ID.
        """
        # complete the provided data with an autogenerated ID:
        data = dto.model_dump()
        data[self._id_field] = await self._id_generator.__anext__()

        # verify the completed data against the full DTO model (modifications might be
        # introduced by the pydantic model):
        full_dto = self._dto_model(**data)

        document = self._dto_to_document(full_dto)
        await self._collection.insert_one(document)

        return full_dto


class MongoDbDaoNaturalId(MongoDbDaoBase[Dto]):
    """A DAO that uses a natural resource ID profided by the client."""

    @classmethod
    def with_transaction(cls) -> AbstractAsyncContextManager["DaoNaturalId[Dto]"]:
        """Creates a transaction manager that uses an async context manager interface:

        Upon __aenter__, pens a new transactional scope. Returns a transaction-scoped
        DAO.

        Upon __aexit__, closes the transactional scope. A full rollback of the
        transaction is performed in case of an exception. Otherwise, the changes to the
        database are committed and flushed.
        """
        raise NotImplementedError()

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
        document = self._dto_to_document(dto)
        try:
            await self._collection.insert_one(document)
        except DuplicateKeyError as error:
            raise ResourceAlreadyExistsError(id_=document["_id"]) from error

    async def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """
        document = self._dto_to_document(dto)
        await self._collection.replace_one(
            {"_id": document["_id"]}, document, upsert=True
        )


class MongoDbConfig(BaseSettings):
    """Configuration parameters for connecting to a MongoDB server.

    Inherit your config class from this class if your application uses MongoDB.
    """

    db_connection_str: SecretStr = Field(
        ...,
        examples=["mongodb://localhost:27017"],
        description=(
            "MongoDB connection string. Might include credentials."
            + " For more information see:"
            + " https://naiveskill.com/mongodb-connection-string/"
        ),
    )
    db_name: str = Field(
        ...,
        examples=["my-database"],
        description="Name of the database located on the MongoDB server.",
    )


class MongoDbDaoFactory(DaoFactoryProtocol):
    """A MongoDB-based provider implementing the DaoFactoryProtocol."""

    def __init__(
        self,
        *,
        config: MongoDbConfig,
    ):
        """Initialize the provider with configuration parameter.

        Args:
            config: MongoDB-specific config parameters.
        """
        self._config = config

        # get a database-specific client:
        self._client: AsyncIOMotorClient = AsyncIOMotorClient(
            self._config.db_connection_str.get_secret_value()
        )
        self._db = self._client[self._config.db_name]

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    @overload
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: None,
        fields_to_index: Optional[Collection[str]],
        id_generator: AsyncGenerator[str, None],
    ) -> DaoNaturalId[Dto]: ...

    @overload
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Collection[str]],
        id_generator: AsyncGenerator[str, None],
    ) -> DaoSurrogateId[Dto, DtoCreation]: ...

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]],
        fields_to_index: Optional[Collection[str]],
        id_generator: AsyncGenerator[str, None],
    ) -> Union[DaoSurrogateId[Dto, DtoCreation], DaoNaturalId[Dto]]:
        """Constructs a DAO for interacting with resources in a MongoDB database.

        Please see the DaoFactoryProtocol superclass for documentation of parameters.

        Please note, the method in this MongoDB-specific implementation of the
        DaoFactoryProtocol would not require to be coroutine. However, other
        implementations of the DaoFactoryProtocol might need to perform await responses
        from the database server. Thus for compliance with the DaoFactoryProtocol, this
        method is async.
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

        if dto_creation_model is None:
            return MongoDbDaoNaturalId(
                collection=collection,
                dto_model=dto_model,
                id_field=id_field,
                document_to_dto=document_to_dto_,
                dto_to_document=dto_to_document_,
            )

        return MongoDbDaoSurrogateId(
            collection=collection,
            dto_model=dto_model,
            dto_creation_model=dto_creation_model,
            id_field=id_field,
            id_generator=id_generator,
            document_to_dto=document_to_dto_,
            dto_to_document=dto_to_document_,
        )

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

"""MongoDB-based provider implementing the DaoFactoryProtocol.

Utilities for testing are located in `./testutils.py`.
"""

from abc import ABC
from collections.abc import AsyncGenerator, AsyncIterator, Collection, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any, Generic, Optional, Union, overload

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
    AsyncIOMotorCollection,
)
from pydantic import BaseSettings, Field, SecretStr

from hexkit.protocols.dao import (
    DaoFactoryProtcol,
    DaoNaturalId,
    DaoSurrogateId,
    Dto,
    DtoCreation,
    DtoCreation_contra,
    InvalidFindMappingError,
    MultipleHitsFoundError,
    ResourceNotFoundError,
    default_uuid4_id_generator,
)
from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = ["MongoDbConfig", "MongoDbDaoFactory"]


class MongoDbDaoBase(ABC, Generic[Dto]):
    """A base class with methods common to all MongoDB-based DAOs.
    This shall be used as base class for other MongoDB-based DAO implementations.
    """

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        id_field: str,
        collection: AsyncIOMotorCollection,
        session: Optional[AsyncIOMotorClientSession] = None,
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
            session:
                If transactional support is needed, please provide an
                AsyncIOMotorClientSession that is within an active transaction. If None
                is provided, every database operation is immediately commited. Defaults
                to None.
        """

        self._collection = collection
        self._session = session
        self._dto_model = dto_model
        self._id_field = id_field

    def _document_to_dto(self, document: dict[str, Any]) -> Dto:
        """Converts a document obtained from the MongoDB database into a DTO model-
        compliant representation."""

        document[self._id_field] = document.pop("_id")
        return self._dto_model(**document)

    def _dto_to_document(self, dto: Dto) -> dict[str, Any]:
        """Converts a DTO into a representation that is compatible documents for a
        MongoDB Database."""

        document = dto.dict()
        document["_id"] = document.pop(self._id_field)

        return document

    async def get(self, *, id_: str) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """

        document = await self._collection.find_one({"_id": id_}, session=self._session)

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
        result = await self._collection.replace_one(
            {"_id": document["_id"]}, document, session=self._session
        )

        if result.matched_count == 0:
            raise ResourceNotFoundError(id_=document["_id"])

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

    async def delete(self, *, id_: str) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """

        result = await self._collection.delete_one({"_id": id_}, session=self._session)

        if result.deleted_count == 0:
            raise ResourceNotFoundError(id_=id_)

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

    def _validate_find_mapping(self, mapping: Mapping[str, Any]):
        """Validates a key/value mapping used in find methods.

        Raises:
            InvalidMappingError: If validation fails.
        """
        try:
            validate_fields_in_model(model=self._dto_model, fields=set(mapping.keys()))
        except FieldNotInModelError as error:
            raise InvalidFindMappingError(
                f"The provided find mapping was invalid: {error}."
            ) from error

    async def find_one(self, *, mapping: Mapping[str, Any]) -> Optional[Dto]:
        """Find the resource that matches the specified mapping. It is expected that
        at most one resource matches the constraints. An exception is raised if multiple
        hits are found.

        Args:
            mapping:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields
            mode:
                One of: "single" (asserts that there will be at most one hit, will raise
                an exception otherwise), "newest" (returns only the resource of the hit
                list that was inserted first), or "oldest" - returns only the resource of
                the hist list that was inserted last. Defaults to "single".

        Returns:
            Returns a hit in the form of the respective DTO model or None if no hit
            was found.

        Raises:
            MultpleHitsFoundError:
                Raised when obtaining more than one hit when using the "single" mode.
        """

        hits = self.find_all(mapping=mapping)

        try:
            document = await hits.__anext__()
        except StopAsyncIteration:
            return None

        try:
            _ = await hits.__anext__()
        except StopAsyncIteration:
            # This is expected:
            return document

        raise MultipleHitsFoundError(mapping=mapping)

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

        self._validate_find_mapping(mapping)

        cursor = self._collection.find(filter=mapping, session=self._session)

        async for document in cursor:
            yield self._document_to_dto(document)


class MongoDbDaoSurrogateId(MongoDbDaoBase[Dto], Generic[Dto, DtoCreation_contra]):
    """A duck type of a DAO that generates an internal/surrogate key for
    indentifying resources in the database. ID/keys cannot be defined by the client of
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
        collection: AsyncIOMotorCollection,
        id_generator: AsyncGenerator[str, None],
        session: Optional[AsyncIOMotorClientSession] = None,
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
            session:
                If transactional support is needed, please provide an
                AsyncIOMotorClientSession that is within an active transaction. If None
                is provided, every database operation is immediately commited. Defaults
                to None.
        """

        super().__init__(
            dto_model=dto_model,
            id_field=id_field,
            collection=collection,
            session=session,
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
        data = dto.dict()
        data[self._id_field] = await self._id_generator.__anext__()

        # verify the completed data against the full DTO model (modifications might be
        # introduced by the pydantic model):
        full_dto = self._dto_model(**data)

        document = self._dto_to_document(full_dto)
        await self._collection.insert_one(document, session=self._session)

        return full_dto


class MongoDbDaoNaturalId(MongoDbDaoBase[Dto]):
    """A duck type of a DAO that uses a natural resource ID profided by the client."""

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
        await self._collection.insert_one(document, session=self._session)

    async def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """

        document = self._dto_to_document(dto)
        await self._collection.replace_one(
            {"_id": document["_id"]}, document, session=self._session, upsert=True
        )


class MongoDbConfig(BaseSettings):
    """Configuration parameters for connecting to a MongoDB server.

    Inherit your config class from this class if your application uses MongoDB."""

    db_connection_str: SecretStr = Field(
        ...,
        example="mongodb://localhost:27017",
        description=(
            "MongoDB connection string. Might include credentials."
            + " For more information see:"
            + " https://naiveskill.com/mongodb-connection-string/"
        ),
    )
    db_name: str = Field(
        ...,
        example="my-database",
        description="Name of the database located on the MongoDB server.",
    )


class MongoDbDaoFactory(DaoFactoryProtcol):
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
        self._client = AsyncIOMotorClient(
            self._config.db_connection_str.get_secret_value()
        )
        self._db = self._client[self._config.db_name]

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    @overload
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: AsyncGenerator[str, None] = default_uuid4_id_generator,
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    async def _get_dao(  # pylint: disable=arguments-differ
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: AsyncGenerator[str, None] = default_uuid4_id_generator,
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]] = None,
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: AsyncGenerator[str, None] = default_uuid4_id_generator,
    ) -> Union[DaoSurrogateId[Dto, DtoCreation], DaoNaturalId[Dto]]:
        """Constructs a DAO for interacting with resources in a MongoDB database.

        Please see the DaoFactoryProtcol superclass for documentation of parameters.

        Please note, the method in this MongoDB-specific implementation of the
        DaoFactoryProtcol would not require to be coroutine. However, other
        implementations of the DaoFactoryProtcol might need to perform await responses
        from the database server. Thus for compliance with the DaoFactoryProtcol, this
        method is async.
        """

        if fields_to_index is not None:
            raise NotImplementedError(
                "Indexing on non-ID fields has not been implemented, yet."
            )

        collection = self._db[name]

        if dto_creation_model is None:
            return MongoDbDaoNaturalId(
                collection=collection,
                dto_model=dto_model,
                id_field=id_field,
            )

        return MongoDbDaoSurrogateId(
            collection=collection,
            dto_model=dto_model,
            dto_creation_model=dto_creation_model,
            id_field=id_field,
            id_generator=id_generator,
        )

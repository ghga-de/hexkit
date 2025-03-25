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

"""MongoDB-based provider implementing the DaoFactoryProtocol.

Utilities for testing are located in `./testutils.py`.
"""

# ruff: noqa: PLR0913

import json
from collections.abc import AsyncIterator, Collection, Mapping
from contextlib import AbstractAsyncContextManager, contextmanager
from datetime import date, datetime
from functools import partial
from pathlib import Path
from typing import Any, Callable, Generic, Optional, Union
from uuid import UUID

from motor.core import AgnosticCollection
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import Field, MongoDsn, PositiveInt, Secret
from pydantic_settings import BaseSettings
from pymongo.errors import DuplicateKeyError, PyMongoError

from hexkit.custom_types import ID
from hexkit.protocols.dao import (
    Dao,
    DaoError,
    DaoFactoryProtocol,
    DbTimeoutError,
    Dto,
    InvalidFindMappingError,
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = [
    "MongoDbConfig",
    "MongoDbDao",
    "MongoDbDaoFactory",
    "translate_pymongo_errors",
]


@contextmanager
def translate_pymongo_errors():
    """Catch PyMongoError and re-raise it as DbTimeoutError if it is a timeout error.

    Non-timeout errors are re-raised as DaoError.
    """
    try:
        yield
    except PyMongoError as exc:
        if exc.timeout:  # denotes timeout-related errors in pymongo
            raise DbTimeoutError(str(exc)) from exc
        raise DaoError(str(exc)) from exc


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


def value_to_document(value: Any) -> Any:
    """Converts the value of a DTO field to the value used in the database.

    This should be compatible with the conversion done in 'dto_to_document'.
    """
    if isinstance(value, (UUID, Path)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def validate_find_mapping(mapping: Mapping[str, Any], *, dto_model: type[Dto]):
    """Validates a key/value mapping used in find methods against the provided DTO model
    by checking if the mapping keys exist as model fields.

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


class MongoDbDao(Generic[Dto]):
    """A MongoDb-specific DAO."""

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        id_field: str,
        collection: AgnosticCollection,
        document_to_dto: Callable[[dict[str, Any]], Dto],
        dto_to_document: Callable[[Dto], dict[str, Any]],
        value_to_document: Callable[[Any], Any],
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
            value_to_document:
                A callable that takes a single DTO value and returns the representation
                of the value that is stored in the MongoDB database.
                This must be compatible with the conversion done by 'dto_to_document'.
        """
        self._collection = collection
        self._dto_model = dto_model
        self._id_field = id_field
        self._document_to_dto = document_to_dto
        self._dto_to_document = dto_to_document
        self._value_to_document = value_to_document

    async def get_by_id(self, id_: ID) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        id_ = self._value_to_document(id_)

        with translate_pymongo_errors():
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
        with translate_pymongo_errors():
            result = await self._collection.replace_one(
                {"_id": document["_id"]}, document
            )

        if result.matched_count == 0:
            raise ResourceNotFoundError(id_=document["_id"])

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

    async def delete(self, id_: ID) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        id_ = self._value_to_document(id_)
        with translate_pymongo_errors():
            result = await self._collection.delete_one({"_id": id_})

        if result.deleted_count == 0:
            raise ResourceNotFoundError(id_=id_)

        # (trusting MongoDB that matching on the _id field can only yield one or
        # zero matches)

    async def find_one(self, *, mapping: Mapping[str, Any]) -> Dto:
        """Find the resource that matches the specified mapping.

        It is expected that at most one resource matches the constraints.
        An exception is raised if no or multiple hits are found.

        The values in the mapping are used to filter the resources, these are
        assumed to be standard JSON scalar types. Particularly, UUIDs and datetimes
        must be represented as strings. Dictionaries can be passed as values to
        specify more complex MongoDB queries.

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

        The values in the mapping are used to filter the resources, these are
        assumed to be standard JSON scalar types. Particularly, UUIDs and datetimes
        must be represented as strings. Dictionaries can be passed as values to
        specify more complex MongoDB queries.

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

        with translate_pymongo_errors():
            cursor = self._collection.find(filter=self._convert_filter_values(mapping))

            async for document in cursor:
                yield self._document_to_dto(document)

    def _convert_filter_values(self, value: Any) -> Any:
        """Convert filter values with non-standard types.

        This makes the values findable in the database where they are stored
        in standard JSON format (i.e. UUID, date and datetime object as strings).

        The passed object can be a scalar value or a dictionary which can be nested.
        """
        if isinstance(value, dict):  # recursively convert all values
            convert = self._convert_filter_values
            return {k: convert(v) for k, v in value.items()}
        if isinstance(value, list):
            convert = self._convert_filter_values
            return [convert(v) for v in value]
        if isinstance(value, tuple):
            convert = self._convert_filter_values
            return tuple(convert(v) for v in value)
        return self._value_to_document(value)

    @classmethod
    def with_transaction(cls) -> AbstractAsyncContextManager["Dao[Dto]"]:
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
        with translate_pymongo_errors():
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
        with translate_pymongo_errors():
            await self._collection.replace_one(
                {"_id": document["_id"]}, document, upsert=True
            )


class MongoDbConfig(BaseSettings):
    """Configuration parameters for connecting to a MongoDB server.

    Inherit your config class from this class if your application uses MongoDB.
    """

    mongo_dsn: Secret[MongoDsn] = Field(
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
    mongo_timeout: Union[PositiveInt, None] = Field(
        default=None,
        examples=[300, 600, None],
        description=(
            "Timeout in seconds for API calls to MongoDB. The timeout applies to all steps"
            + " needed to complete the operation, including server selection, connection"
            + " checkout, serialization, and server-side execution. When the timeout"
            + " expires, PyMongo raises a timeout exception. If set to None, the"
            + " operation will not time out (default MongoDB behavior)."
        ),
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
        timeout_ms = (
            int(config.mongo_timeout * 1000)
            if config.mongo_timeout is not None
            else None
        )

        # get a database-specific client:
        self._client: AsyncIOMotorClient = AsyncIOMotorClient(
            str(self._config.mongo_dsn.get_secret_value()),
            timeoutMS=timeout_ms,
        )
        self._db = self._client[self._config.db_name]

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__qualname__}(config={repr(self._config)})"

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]],
    ) -> Dao[Dto]:
        """Constructs a DAO for interacting with resources in a MongoDB database.

        Please see the DaoFactoryProtocol superclass for documentation of parameters.

        Please note, the method in this MongoDB-specific implementation of the
        DaoFactoryProtocol would not normally be required to be async. However, other
        implementations of the DaoFactoryProtocol might need to perform await responses
        from the database server. Thus for compliance with the DaoFactoryProtocol, this
        method is async.
        """
        if fields_to_index is not None:
            raise NotImplementedError(
                "Indexing on non-ID fields has not been implemented, yet."
            )

        collection = self._db[name]

        return MongoDbDao(
            collection=collection,
            dto_model=dto_model,
            id_field=id_field,
            document_to_dto=partial(
                document_to_dto, id_field=id_field, dto_model=dto_model
            ),
            dto_to_document=partial(dto_to_document, id_field=id_field),
            value_to_document=value_to_document,
        )

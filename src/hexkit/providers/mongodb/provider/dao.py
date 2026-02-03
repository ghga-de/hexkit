# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from collections.abc import AsyncIterator, Callable, Collection, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import partial
from typing import Any, Generic, Literal, TypeAlias

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from hexkit.custom_types import ID
from hexkit.protocols.dao import (
    Dao,
    DaoFactoryProtocol,
    Dto,
    IndexBase,
    InvalidFindMappingError,
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    UniqueConstraintViolationError,
)
from hexkit.providers.mongodb.config import MongoDbConfig
from hexkit.providers.mongodb.provider.client import ConfiguredMongoClient
from hexkit.providers.mongodb.provider.utils import (
    document_to_dto,
    dto_to_document,
    translate_pymongo_errors,
)
from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = [
    "MongoDbDao",
    "MongoDbDaoFactory",
    "MongoDbIndex",
    "get_single_hit",
    "replace_id_field_in_find_mapping",
    "validate_find_mapping",
]


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


FieldName: TypeAlias = str
SortOrder: TypeAlias = Literal[1] | Literal[-1]


class MongoDbIndex(IndexBase):
    """Information required to apply a single MongoDbIndex."""

    fields: dict[FieldName, SortOrder]
    properties: dict[str, Any] | None = None

    def __init__(
        self,
        *,
        fields: FieldName | dict[FieldName, SortOrder],
        properties: dict[str, Any] | None = None,
    ):
        """Initialize the index.

        **Args**
        - `fields`: a single field name or a dict containing the field name and the sort
            order. For sort order, 1 means ascending and -1 means descending. If the index
            covers the model's `id_field`, then specify the actual field name instead of
            "_id". When passing a field name instead of a dict, ascending sort order is used.
        - `properties`: a dictionary where the keys are MongoDB index property names, and
            the values are the value to pass for that property.

        More information on index creation with Pymongo, including a list of the supported
        index properties, can be found [here](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.create_index)

        Example:
        ```python
        # This creates a compound unique index over field_a and field_b.
        MongoDbIndex(
            fields={"field_a": 1, "field_b": -1},
            properties={"unique": True}
        )

        # This creates an ascending-sorted index over field_a.
        MongoDbIndex("field_a")
        ```
        """
        if isinstance(fields, str):
            self.fields = {fields: 1}
        else:
            self.fields = fields

        self.properties = properties

    def list_field_names(self) -> list[str]:
        """Return a list of all the field names contained in this index"""
        return list(self.fields)

    def fields_with_id_replaced(
        self, id_field: str
    ) -> str | list[tuple[FieldName, SortOrder]]:
        """Returns the `fields` property with all instances of `id_field`
        replaced with `"_id"`.
        """
        return [("_id" if f == id_field else f, so) for f, so in self.fields.items()]


class MongoDbDao(Generic[Dto]):
    """A MongoDb-specific DAO."""

    def __init__(
        self,
        *,
        dto_model: type[Dto],
        id_field: str,
        collection: AsyncCollection,
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
                A collection object from the pymongo async library.
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

    async def get_by_id(self, id_: ID) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
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
            try:
                result = await self._collection.replace_one(
                    {"_id": document["_id"]}, document
                )
            except DuplicateKeyError as error:
                key_value = error.details.get("keyValue", {})  # type: ignore
                raise UniqueConstraintViolationError(unique_fields=key_value) from error

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
            cursor = self._collection.find(filter=mapping)

            async for document in cursor:
                yield self._document_to_dto(document)

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
            UniqueConstraintViolationError:
                when inserting the dto would violate a unique index constraint over some
                field other than the ID field.
        """
        document = self._dto_to_document(dto)
        with translate_pymongo_errors():
            try:
                await self._collection.insert_one(document)
            except DuplicateKeyError as error:
                key_value = error.details.get("keyValue")  # type: ignore
                if key_value is not None and list(key_value) != ["_id"]:
                    raise UniqueConstraintViolationError(
                        unique_fields=key_value
                    ) from error
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
            try:
                await self._collection.replace_one(
                    {"_id": document["_id"]}, document, upsert=True
                )
            except DuplicateKeyError as error:
                key_value = error.details.get("keyValue", {})  # type: ignore
                raise UniqueConstraintViolationError(unique_fields=key_value) from error


class MongoDbDaoFactory(DaoFactoryProtocol[MongoDbIndex]):
    """A MongoDB-based provider implementing the DaoFactoryProtocol."""

    @classmethod
    @asynccontextmanager
    async def construct(
        cls,
        *,
        config: MongoDbConfig,
    ):
        """Yields a MongoDbDaoFactory instance with the provided configuration.

        The client connection is established and closed automatically.
        """
        async with ConfiguredMongoClient(config=config) as client:
            # The client is an instance of AsyncMongoClient
            yield cls(config=config, client=client)

    def __init__(
        self,
        *,
        config: MongoDbConfig,
        client: AsyncMongoClient,
    ):
        """Initialize the provider with configuration parameter.

        Args:
            config: MongoDB-specific config parameters.
            client: An instance of an async MongoDB client.
        """
        self._config = config
        self._db = client.get_database(self._config.db_name)

    def __repr__(self) -> str:  # noqa: D105
        return f"{self.__class__.__name__}(config={repr(self._config)})"

    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        indexes: Collection[MongoDbIndex] | None = None,
    ) -> Dao[Dto]:
        """Constructs a DAO for interacting with resources in a MongoDB database.

        Please see the DaoFactoryProtocol superclass for documentation of parameters.

        Please note, the method in this MongoDB-specific implementation of the
        DaoFactoryProtocol would not normally be required to be async. However, other
        implementations of the DaoFactoryProtocol might need to perform await responses
        from the database server. Thus for compliance with the DaoFactoryProtocol, this
        method is async.
        """
        collection = self._db[name]

        for index in indexes or []:
            properties = index.properties or {}
            await collection.create_index(
                index.fields_with_id_replaced(id_field=id_field), **properties
            )

        return MongoDbDao(
            collection=collection,
            dto_model=dto_model,
            id_field=id_field,
            document_to_dto=partial(
                document_to_dto, id_field=id_field, dto_model=dto_model
            ),
            dto_to_document=partial(dto_to_document, id_field=id_field),
        )

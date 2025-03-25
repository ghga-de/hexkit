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

"""Protocol for creating Data Access Objects to perform CRUD (plus find) interactions
with the database.
"""

# ruff: noqa: PLR0913

import typing
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Collection, Mapping
from contextlib import AbstractAsyncContextManager
from functools import partial
from typing import Any, Optional, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field

from hexkit.custom_types import ID
from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = [
    "Dao",
    "DaoFactoryProtocol",
    "FindError",
    "MultipleHitsFoundError",
    "ResourceAlreadyExistsError",
    "ResourceNotFoundError",
    "UUID4Field",
]

# Type variable for handling Data Transfer Objects:
Dto = TypeVar("Dto", bound=BaseModel)


class DaoError(RuntimeError):
    """Base for all errors related to DAO operations."""


class ResourceNotFoundError(DaoError):
    """Raised when a requested resource did not exist."""

    def __init__(self, *, id_: ID):
        message = f'The resource with the id "{id_}" does not exist.'
        super().__init__(message)


class ResourceAlreadyExistsError(DaoError):
    """Raised when a resource did unexpectedly exist."""

    def __init__(self, *, id_: ID):
        message = f'The resource with the id "{id_}" already exists.'
        super().__init__(message)


class FindError(DaoError):
    """Base for all error related to DAO find operations."""


class InvalidFindMappingError(FindError):
    """Raised when an invalid mapping was passed provided to find."""


class MultipleHitsFoundError(FindError):
    """Raised when a DAO find operation did result in multiple hits while only a
    single hit was expected.
    """

    def __init__(self, *, mapping: Mapping[str, str]):
        message = (
            "Multiple hits were found for the following key-value pairs while only a"
            f" single one was expected: {mapping}"
        )
        super().__init__(message)


class NoHitsFoundError(FindError):
    """Raised when a DAO find operation did result in no hits while a
    single hit was expected.
    """

    def __init__(self, *, mapping: Mapping[str, str]):
        message = (
            "No hits were found for the following key-value pairs while a single one"
            f" was expected: {mapping}"
        )
        super().__init__(message)


class DbTimeoutError(DaoError):
    """Raised when a database operation timed out."""

    def __init__(self, details: str):
        message = f"The database operation timed out: {details}"
        super().__init__(message)


# provide standardized default factory for UUID4 fields
UUID4Field = partial(Field, default_factory=uuid4)


class Dao(typing.Protocol[Dto]):
    """A duck type with methods common to all DAOs."""

    @classmethod
    def with_transaction(cls) -> AbstractAsyncContextManager["Dao[Dto]"]:
        """Creates a transaction manager that uses an async context manager interface:

        Upon __aenter__, pens a new transactional scope. Returns a transaction-scoped
        DAO.

        Upon __aexit__, closes the transactional scope. A full rollback of the
        transaction is performed in case of an exception. Otherwise, the changes to the
        database are committed and flushed.
        """
        ...

    async def get_by_id(self, id_: ID) -> Dto:
        """Get a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        ...

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
        ...

    async def delete(self, id_: ID) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        ...

    async def find_one(self, *, mapping: Mapping[str, Any]) -> Dto:
        """Find the resource that matches the specified mapping.

        It is expected that at most one resource matches the constraints.
        An exception is raised if no or multiple hits are found.

        The values in the mapping are used to filter the resources, these are
        assumed to be standard JSON scalar types. Particularly, UUIDs and datetimes
        must be represented as strings. The behavior for non-scalars types depends
        on the specific provider.

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
        ...

    def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[Dto]:
        """Find all resources that match the specified mapping.

        The values in the mapping are used to filter the resources, these are
        assumed to be standard JSON scalar types. Particularly, UUIDs and datetimes
        must be represented as strings. The behavior for non-scalar types depends
        on the specific provider.

        Args:
            mapping:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields.

        Returns:
            An AsyncIterator of hits. All hits are in the form of the respective DTO
            model.
        """
        ...

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
        ...

    async def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """
        ...


class DaoFactoryBase:
    """A base for Data Access Objects (DAO) Factory protocols."""

    class IdFieldNotFoundError(TypeError):
        """Raised when the dto_model did not contain the expected id_field."""

    class IdTypeNotSupportedError(TypeError):
        """Raised when the id_field of the dto_model has an unexpected Type."""

    class IndexFieldsInvalidError(ValueError):
        """Raised when providing an invalid list of fields to index."""

    @classmethod
    def _validate_dto_model_id(cls, *, dto_model: type[Dto], id_field: str) -> None:
        """Checks whether the dto_model contains the expected id_field.
        Raises IdFieldNotFoundError otherwise.
        """
        properties = dto_model.model_json_schema()["properties"]
        schema = properties.get(id_field)
        if schema is None:
            raise cls.IdFieldNotFoundError()
        id_type = schema.get("type")
        if id_type not in ("integer", "string"):
            raise cls.IdTypeNotSupportedError()

    @classmethod
    def _validate_fields_to_index(
        cls,
        *,
        dto_model: type[Dto],
        fields_to_index: Optional[Collection[str]],
    ) -> None:
        """Checks that all provided fields are present in the dto_model.
        Raises IndexFieldsInvalidError otherwise.
        """
        if fields_to_index is None:
            return

        try:
            validate_fields_in_model(model=dto_model, fields=fields_to_index)
        except FieldNotInModelError as error:
            raise cls.IndexFieldsInvalidError(
                f"Provided index fields are invalid: {error}"
            ) from error

    @classmethod
    def _validate(
        cls,
        *,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]],
    ) -> None:
        """Validates the input parameters of the get_dao method."""
        cls._validate_dto_model_id(dto_model=dto_model, id_field=id_field)
        cls._validate_fields_to_index(
            dto_model=dto_model, fields_to_index=fields_to_index
        )


class DaoFactoryProtocol(DaoFactoryBase, ABC):
    """A protocol describing a factory to produce Data Access Objects (DAO) objects
    that are enclosed in transactional scopes.
    """

    async def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]] = None,
    ) -> Dao[Dto]:
        """Constructs a DAO for interacting with resources in a database.

        Args:
            name:
                The name of the resource type (roughly equivalent to the name of a
                database table or collection).
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            fields_to_index:
                Optionally, provide any fields that should be indexed in addition to the
                `id_field`. Defaults to None.
        Returns:
            A DAO specific to the provided DTO model.

        Raises:
            self.IdFieldNotFoundError:
                Raised when the dto_model did not contain the expected id_field.
            self.IdTypeNotSupportedError:
                Raised when the id_field of the dto_model has an unexpected type.
        """
        self._validate(
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
        )

        return await self._get_dao(
            name=name,
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
        )

    @abstractmethod
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]],
    ) -> Dao[Dto]:
        """*To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

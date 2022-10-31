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

"""Protocol for creating Data Access Objects to perform CRUD (plus find) interactions
with the database."""

import typing
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Collection, Mapping
from contextlib import AbstractAsyncContextManager
from copy import copy
from typing import Any, Optional, TypeVar, Union, overload
from uuid import uuid4

from pydantic import BaseModel

from hexkit.utils import FieldNotInModelError, validate_fields_in_model

__all__ = [
    "ResourceNotFoundError",
    "ResourceAlreadyExistsError",
    "FindError",
    "MultipleHitsFoundError",
    "DaoNaturalId",
    "DaoSurrogateId",
    "DaoFactoryProtocol",
    "uuid4_id_generator",
]

# Type variables for handling Data Transfer Objects:
Dto = TypeVar("Dto", bound=BaseModel)
DtoCreation = TypeVar("DtoCreation", bound=BaseModel)
DtoCreation_contra = TypeVar("DtoCreation_contra", bound=BaseModel, contravariant=True)


class ResourceNotFoundError(RuntimeError):
    """Raised when a requested resource did not exist."""

    def __init__(self, *, id_: str):
        message = f'The resource with the id "{id_}" does not exist.'
        super().__init__(message)


class ResourceAlreadyExistsError(RuntimeError):
    """Raised when a resource did unexpectedly exist."""

    def __init__(self, *, id_: str):
        message = f'The resource with the id "{id_}" does already exist.'
        super().__init__(message)


class FindError(RuntimeError):
    """Base for all error related to DAO find operations."""


class InvalidFindMappingError(FindError):
    """Raised when an invalid mapping was passed provided to find."""


class MultipleHitsFoundError(FindError):
    """Raised when a DAO find operation did result in multiple hits while only a
    single hit was expected."""

    def __init__(self, *, mapping: Mapping[str, str]):
        message = (
            "Multiple hits were found for the following key-value pairs while only a"
            f" single one was expected: {mapping}"
        )
        super().__init__(message)


class NoHitsFoundError(FindError):
    """Raised when a DAO find operation did result in no hits while a
    single hit was expected."""

    def __init__(self, *, mapping: Mapping[str, str]):
        message = (
            "No hits were found for the following key-value pairs while a single one"
            f" was expected: {mapping}"
        )
        super().__init__(message)


class DaoCommons(typing.Protocol[Dto]):
    """A duck type with methods common to all DAOs. This shall be used as base class for
    other DAO duck types.
    """

    async def get_by_id(self, id_: str) -> Dto:
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

    async def delete(self, *, id_: str) -> None:
        """Delete a resource by providing its ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        ...

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
        ...

    def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[Dto]:
        """Find all resources that match the specified mapping.

        Args:
            mapping:
                A mapping where the keys correspond to the names of resource fields
                and the values correspond to the actual values of the resource fields.

        Returns:
            An AsyncIterator of hits. All hits are in the form of the respective DTO
            model.
        """
        ...


class DaoSurrogateId(DaoCommons[Dto], typing.Protocol[Dto, DtoCreation_contra]):
    """A duck type of a DAO that generates an internal/surrogate key for
    identifying resources in the database. ID/keys cannot be defined by the client of
    the DAO. Thus, both a standard DTO model (first type parameter), which includes
    the key field, as well as special DTO model (second type parameter), which is
    identical to the first one, but does not include the ID field and is dedicated for
     creation of new resources.
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
        ...

    async def insert(self, dto: DtoCreation_contra) -> Dto:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object without the
                resource ID (which will be set automatically).

        Returns:
            Returns a copy of the newly inserted resource including its assigned ID.
        """
        ...


class DaoNaturalId(DaoCommons[Dto], typing.Protocol[Dto]):
    """A duck type of a DAO that uses a natural resource ID provided by the client."""

    @classmethod
    def with_transaction(cls) -> AbstractAsyncContextManager["DaoNaturalId[Dto]"]:
        """Creates a transaction manager that uses an async context manager interface:

        Upon __aenter__, pens a new transactional scope. Returns a transaction-scoped
        DAO.

        Upon __aexit__, closes the transactional scope. A full rollback of the
        transaction is performed in case of an exception. Otherwise, the changes to the
        database are committed and flushed.
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


async def uuid4_id_generator() -> AsyncGenerator[str, None]:
    """Generates a new ID using the UUID4 algorithm.
    This is an AsyncGenerator to be compliant with the id_generator requirements of the
    DaoFactoryProtocol.
    """

    while True:
        yield str(uuid4())


class DaoFactoryProtocol(ABC):
    """A protocol describing a factory to produce Data Access Objects (DAO) objects
    that are enclosed in transactional scopes.
    """

    class IdFieldNotFoundError(ValueError):
        """Raised when the dto_model did not contain the expected id_field."""

    class CreationModelInvalidError(ValueError):
        """Raised when the DtoCreationModel was invalid in relation to the main
        DTO model."""

    class IndexFieldsInvalidError(ValueError):
        """Raised when providing an invalid list of fields to index."""

    @classmethod
    def _validate_dto_model_id(cls, *, dto_model: type[Dto], id_field: str) -> None:
        """Checks whether the dto_model contains the expected id_field.
        Raises IdFieldNotFoundError otherwise."""

        if id_field not in dto_model.schema()["properties"]:
            raise cls.IdFieldNotFoundError()

    @classmethod
    def _validate_dto_creation_model(
        cls,
        *,
        dto_model: type[Dto],
        dto_creation_model: Optional[type[DtoCreation]],
        id_field: str,
    ) -> None:
        """Checks that the dto_creation_model has the same fields as the dto_model
        except missing the ID. Raises CreationModelInvalidError otherwise."""

        if dto_creation_model is None:
            return

        expected_properties = copy(dto_model.schema()["properties"])
        # (the schema method returns an attribute of the class, making a copy to not
        # alter the class)
        del expected_properties[id_field]
        observed_properties = dto_creation_model.schema()["properties"]

        if observed_properties != expected_properties:
            raise cls.CreationModelInvalidError()

    @classmethod
    def _validate_fields_to_index(
        cls,
        *,
        dto_model: type[Dto],
        fields_to_index: Optional[Collection[str]],
    ) -> None:
        """Checks that all provided fields are present in the dto_model.
        Raises IndexFieldsInvalidError otherwise."""

        if fields_to_index is None:
            return

        try:
            validate_fields_in_model(model=dto_model, fields=fields_to_index)
        except FieldNotInModelError as error:
            raise cls.IndexFieldsInvalidError(
                f"Provided index fields are invalid: {error}"
            ) from error

    @overload
    async def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: Optional[AsyncGenerator[str, None]] = None,
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    async def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: Optional[AsyncGenerator[str, None]] = None,
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    async def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]] = None,
        fields_to_index: Optional[Collection[str]] = None,
        id_generator: Optional[AsyncGenerator[str, None]] = None,
    ) -> Union[DaoSurrogateId[Dto, DtoCreation], DaoNaturalId[Dto]]:
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
            dto_creation_model:
                An optional DTO model specific for creation of a new resource. This
                model has to be identical to the `dto_model` except that it has to miss
                the `id_field`. If specified, the resource ID will be generated by the DAO
                implementation upon resource creation. Otherwise (if set to None), resource IDs
                have to be specified upon resource creation. Defaults to None.
            id_generator:
                A generator that yields strings that will be used as IDs when creating
                new resources. Please note, each ID should be unique. Moreover, the
                generator should never exhaust.
                By default a UUID4-based generator is used.
        Returns:
            If a dedicated `dto_creation_model` is specified, a DAO of type
            DaoSurrogateID, which autogenerates IDs upon resource creation, is returned.
            Otherwise, returns a DAO of type DaoNaturalId, which require ID
            specification upon resource creation.

        Raises:
            self.CreationModelInvalidError:
                Raised when the DtoCreationModel was invalid in relation to the main
                DTO model.
            self.IdFieldNotFoundError:
                Raised when the dto_model did not contain the expected id_field.
        """

        self._validate_dto_model_id(dto_model=dto_model, id_field=id_field)

        self._validate_dto_creation_model(
            dto_model=dto_model,
            dto_creation_model=dto_creation_model,
            id_field=id_field,
        )

        self._validate_fields_to_index(
            dto_model=dto_model, fields_to_index=fields_to_index
        )

        if id_generator is None:
            # instanciate the default ID generator:
            id_generator = uuid4_id_generator()

        return await self._get_dao(
            name=name,
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
            dto_creation_model=dto_creation_model,
            # (above behavior by mypy seems incorrect)
            id_generator=id_generator,
        )

    @overload
    @abstractmethod
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: None,
        fields_to_index: Optional[Collection[str]],
        id_generator: AsyncGenerator[str, None],
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    @abstractmethod
    async def _get_dao(  # pylint: disable=arguments-differ
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Collection[str]],
        id_generator: AsyncGenerator[str, None],
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    @abstractmethod
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
        """*To be implemented by the provider. Input validation is done outside of this
        method.*"""
        ...

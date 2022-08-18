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
from copy import deepcopy
from typing import Literal, Mapping, Optional, Sequence, TypeVar, Union, overload

from pydantic import BaseModel

__all__ = [
    "ResourceNotFoundError",
    "ResourceAlreadyExistsError",
    "FindError",
    "NoHitsFoundError",
    "MultpleHitsFoundError",
    "DaoNaturalId",
    "DaoSurrogateId",
    "DaoFactoryProtcol",
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


class NoHitsFoundError(FindError):
    """Raised when a DAO find operation did not result in any hits but at least one
    hit was expected."""

    def __init__(self, *, kv: Mapping[str, str]):
        message = f"No match was found for key-value pairs: {kv}"
        super().__init__(message)


class MultpleHitsFoundError(FindError):
    """Raised when a DAO find operation did result in multiple hits while only a
    single hit was expected."""

    def __init__(self, *, kv: Mapping[str, str]):
        message = (
            "Multiple hits were found for the following key-value pairs while only a"
            f" single one was expected: {kv}"
        )
        super().__init__(message)


class DaoCommons(typing.Protocol[Dto]):
    """A duck type with methods common to all DAOs. This shall be used as base class for
    other DAO duck types.
    """

    def get(self, *, id_: str) -> Dto:
        """Get a resource by providing it's ID.

        Args:
            id_: The ID of the resource.

        Returns:
            The resource represented using the respective DTO model.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """
        ...

    def update(self, dto: Dto) -> None:
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

    def delete(self, *, id_: str) -> None:
        """Delete a resource by providing it's ID.

        Args:
            id_: The ID of the resource.

        Raises:
            ResourceNotFoundError: when resource with the specified id_ was not found
        """

    @overload
    def find(
        self, *, kv: Mapping[str, object], returns: Literal["all"]
    ) -> Sequence[Dto]:
        ...

    @overload
    def find(
        self,
        *,
        kv: Mapping[str, object],
        returns: Literal["newest", "oldest", "single"],
    ) -> Dto:
        ...

    def find(
        self,
        *,
        kv: Mapping[str, object],
        returns: Literal["all", "newest", "oldest", "single"] = "all",
    ) -> Union[Sequence[Dto], Dto]:
        """Find resource by specifing a list of key-value pairs that must match.

        Args:
            kv:
                A mapping where the keys correspond to the names of resource fields
                and the values corresponds to the actual values of the resource fields.
            returns:
                Controls the return behavior. Can be one of: "all" - returns all hits;
                "newest" - returns only the resource of the hit list that was inserted
                first); "oldest" - returns only the resource of the hist list that was
                inserted last; "single" - asserts that there will only be one hit
                (will raise an exception otherwise). Defaults to "all".

        Returns:
            If `returns` was set to "all", a sequence of hits is returned. Otherwise will
            return only a single hit. All hits are in the form of the respective DTO
            model.

        Raises:
            NoHitsFoundError:
                Raised when no hits where found when used in "newest", "oldest", or
                "single" mode. When using the "all" mode, zero hits will not cause an
                exception but simply result in an empty list beeing returned.
            MultpleHitsFoundError:
                Raised when obtaining more than one hit when using the "single" mode.
        """
        ...


class DaoSurrogateId(DaoCommons[Dto], typing.Protocol[Dto, DtoCreation_contra]):
    """A duck type of a DAO that uses that generates an internal/surrogate key for
    indentifying resources in the database. ID/keys cannot be defined by the client of
    the DAO. Thus, both a standard DTO model (first type variable), which includes the
    key field, as well as special DTO model (second type variable), which is identical
    to the does not include the ID field and is dedicated for creation of new resources,
    is needed.
    """

    def insert(self, dto: DtoCreation_contra) -> Dto:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object without the
                resource ID (which will be set automatically).

        Returns:
            Returns a copy of the newly inserted resource including it assigned ID.
        """
        ...


class DaoNaturalId(DaoCommons[Dto], typing.Protocol[Dto]):
    """A duck type of a DAO that uses natural resource ID profided by the client."""

    def insert(self, dto: Dto) -> None:
        """Create a new resource.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.

        Raises:
            ResourceAlreadyExistsError:
                when a resource with the id specified in the dto does already exist.
        """
        ...

    def upsert(self, dto: Dto) -> None:
        """Update the provided resource if it already exists, create it otherwise.

        Args:
            dto:
                Resource content as a pydantic-based data transfer object including the
                resource ID.
        """
        ...


class DaoFactoryProtcol(ABC):
    """A Protocol describing a factory to produce Data Access Objects (DAO) objects when
    providing a Data Transfer Objetct (DTO) class.
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
        Raises otherwises."""

        if id_field not in dto_model.schema()["properties"].keys():
            raise cls.IdFieldNotFoundError()

    @classmethod
    def _validate_dto_creation_model(
        cls,
        *,
        dto_model: type[Dto],
        dto_creation_model: Optional[type[DtoCreation]],
        id_field: str,
    ) -> None:
        """Checks that the dto_creation_model has identical fields than the dto_model
        except missing the ID. Raises otherwises."""

        if dto_creation_model is None:
            return

        expected_properties = deepcopy(dto_model.schema()["properties"])
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
        fields_to_index: Optional[Sequence[str]],
    ) -> None:
        """Checks that all index fields are present in the dto_model. Raises otherwises."""

        if fields_to_index is None:
            return

        existing_fields = dto_model.schema()["properties"].keys()

        for field in fields_to_index:
            if field not in existing_fields:
                raise cls.IndexFieldsInvalidError(f"Field '{field}' not in DTO model.")

    @overload
    def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Sequence[str]] = None,
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Sequence[str]] = None,
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]] = None,
        fields_to_index: Optional[Sequence[str]] = None,
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
                the `id_field`. If specified, resource ID will be generated by the DAO
                implementation upon. Otherwise (if set to None), resource IDs have to
                be specified upon resource creation. Defaults to None.
        Returns:
            If a dedicated `dto_creation_model` is specified, a DAO of type
            DaoSurrogateID, which autogenerates IDs upon resource creation, is returned.
            Otherwise, returns a DAO of type DaoNaturalId, which require ID
            specification upon resource creation.

        Raises:
            self.DtoCreationModelInvalidInvalid:
                Raised when the DtoCreationModel was invalid in relation to the main
                DTO model.
            self.DtoIdFieldNotFoundError:
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

        return self._get_dao(
            name=name,
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
            dto_creation_model=dto_creation_model,  # type: ignore
            # (above behavior by mypy seems incorrect)
        )

    @overload
    def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Sequence[str]] = None,
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: type[DtoCreation],
        fields_to_index: Optional[Sequence[str]] = None,
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    @abstractmethod
    def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        dto_creation_model: Optional[type[DtoCreation]] = None,
        fields_to_index: Optional[Sequence[str]] = None,
    ) -> Union[DaoSurrogateId[Dto, DtoCreation], DaoNaturalId[Dto]]:
        """*To be implemented by the provider. Input validation is done outside of this
        method.*"""
        ...

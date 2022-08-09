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

from abc import ABC, abstractmethod
import typing
from typing import Literal, Optional, Generic, Sequence, TypeVar, Union, overload
from enum import Enum

from pydantic import BaseModel


Dto = TypeVar("Dto", bound=BaseModel, covariant=True, contravariant=False)
DtoCreation = TypeVar(
    "DtoCreation", bound=BaseModel, covariant=True, contravariant=False
)


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
        """
        ...

    def delete(self, *, id_: str) -> None:
        """Delete a resource by providing it's ID.

        Args:
            id_: The ID of the resource.
        """

    @overload
    def find(self, *, kv: dict[str, object], returns: Literal["all"]) -> Sequence[Dto]:
        ...

    @overload
    def find(
        self, *, kv: dict[str, object], returns: Literal["newest", "oldest", "single"]
    ) -> Dto:
        ...

    def find(
        self,
        *,
        kv: dict[str, object],
        returns: Literal["all", "newest", "oldest", "single"] = "all",
    ) -> Union[Sequence[Dto], Dto]:
        """Find resource by specifing a list of key-value pairs that must match.

        Args:
            kv:
                A dictionary where the keys correspond to the name of resource fields
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
        """
        ...


class DaoSurrogateId(DaoCommons[Dto], typing.Protocol[Dto, DtoCreation]):
    """A duck type of a DAO that uses that generates an internal/surrogate key for
    indentifying resources in the database. ID/keys cannot be defined by the client of
    the DAO. Thus, both a standard DTO model (first type variable), which includes the
    key field, as well as special DTO model (second type variable), which is identical
    to the does not include the ID field and is dedicated for creation of new resources,
    is needed.
    """


class DaoNaturalId(DaoCommons[Dto], typing.Protocol[Dto]):
    """A duck type of a DAO that uses natural resource ID profided by the client."""


class DaoFactoryProtcol(ABC):
    """A Protocol describing a factory to produce Data Access Objects (DAO) objects when
    providing a Data Transfer Objetct (DTO) class.
    """

    @overload
    def get_dao(
        self, *, name: str, dto_model: Dto, id_field: str, fields_to_index: list[str]
    ) -> DaoNaturalId[Dto]:
        ...

    @overload
    def get_dao(
        self,
        *,
        name: str,
        dto_model: Dto,
        id_field: str,
        fields_to_index: list[str],
        dto_creation_model: DtoCreation,
    ) -> DaoSurrogateId[Dto, DtoCreation]:
        ...

    def get_dao(
        self,
        *,
        name: str,
        dto_model: Dto,
        id_field: str,
        fields_to_index: list[str] = [],
        dto_creation_model: Optional[DtoCreation] = None,
    ) -> Union[DaoSurrogateId[Dto, DtoCreation], DaoNaturalId[Dto]]:
        """Constructs a DAO for interacting with resources in a database.

        Args:
            name:
                The name of the resource type (roughly equivalent to the name of a
                database table or collection).
            dto_model:
                A pydantic model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            fields_to_index:
                Optionally, provide any fields that should be indexed in addition to the
                `id_field`. Defaults to `[]`.
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
        """
        ...

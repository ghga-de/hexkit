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

"""Testing the dao factory protocol."""

from typing import Optional, Sequence, Union, overload

import pytest
from pydantic import BaseModel

from hexkit.protocols.dao import (
    DaoFactoryProtcol,
    DaoNaturalId,
    DaoSurrogateId,
    Dto,
    DtoCreation,
)


class FakeDaoFactory(DaoFactoryProtcol):
    """Implements the DaoFactoryProtocol without providing any logic."""

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

        raise NotImplementedError()


class ExampleCreationDto(BaseModel):
    """Example DTO creation model."""

    some_param: str
    another_param: int


class ExampleInvalidCreationDto(ExampleCreationDto):
    """Example for an DTO creation model that is invalid because it contains an
    parameter that the main DTO model is missing."""

    unexpected_param: str


class ExampleDto(ExampleCreationDto):
    """Example DTO model."""

    id: str


@pytest.mark.asyncio
async def test_get_dto_valid():
    """Use the get_dao method of the DaoFactory with valid parameters."""

    dao_factory = FakeDaoFactory()

    with pytest.raises(NotImplementedError):
        _ = dao_factory.get_dao(
            name="test_dao",
            dto_model=ExampleDto,
            id_field="id",
            fields_to_index=["some_param"],
            dto_creation_model=ExampleCreationDto,
        )


@pytest.mark.asyncio
async def test_get_dto_invalid_id():
    """Use the get_dao method of the DaoFactory with and invalid ID that is not found in
    the provided DTO model."""

    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtcol.IdFieldNotFoundError):
        _ = dao_factory.get_dao(
            name="test_dao", dto_model=ExampleDto, id_field="invalid_id"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dto_creation_model",
    [ExampleDto, ExampleInvalidCreationDto],
)
async def test_get_dto_invalid_creation_model(dto_creation_model: type[BaseModel]):
    """Use the get_dao method of the DaoFactory with and invalid creation model."""

    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtcol.CreationModelInvalidError):
        _ = dao_factory.get_dao(
            name="test_dao",
            dto_model=ExampleDto,
            id_field="id",
            dto_creation_model=dto_creation_model,
        )
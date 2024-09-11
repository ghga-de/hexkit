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

"""Testing the DAO factory protocol."""

from collections.abc import Collection
from typing import Optional

import pytest
from pydantic import UUID4, BaseModel

from hexkit.protocols.dao import Dao, DaoFactoryProtocol, Dto, UUID4Field

pytestmark = pytest.mark.asyncio()


class FakeDaoFactory(DaoFactoryProtocol):
    """Implements the DaoFactoryProtocol without providing any logic."""

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
        raise NotImplementedError()


class ExampleDto(BaseModel):
    """Example DTO model."""

    id: UUID4 = UUID4Field(description="The ID of the resource.")
    str_field: str
    int_field: int
    bool_field: bool


async def test_get_dto_valid():
    """Use the get_dao method of the DaoFactory with valid parameters."""
    dao_factory = FakeDaoFactory()

    for id_field in "id", "str_field", "int_field":
        # should raise a NotImplementedError because indexing is not yet implemented,
        # but the parameters should be considered valid
        with pytest.raises(NotImplementedError):
            _ = await dao_factory.get_dao(
                name="test_dao",
                dto_model=ExampleDto,
                id_field=id_field,
                fields_to_index={"str_field", "int_field"},
            )


async def test_get_dto_invalid_id():
    """Use the get_dao method of the DaoFactory with an invalid ID that is not found in
    the provided DTO model or has the wrong type.
    """
    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtocol.IdFieldNotFoundError):
        _ = await dao_factory.get_dao(
            name="test_dao", dto_model=ExampleDto, id_field="non_existing_field"
        )

    with pytest.raises(DaoFactoryProtocol.IdTypeNotSupportedError):
        _ = await dao_factory.get_dao(
            name="test_dao", dto_model=ExampleDto, id_field="bool_field"
        )


async def test_get_dto_invalid_fields_to_index():
    """Use the get_dao method of the DaoFactory with an invalid list of fields to index."""
    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtocol.IndexFieldsInvalidError):
        _ = await dao_factory.get_dao(
            name="test_dao",
            dto_model=ExampleDto,
            id_field="id",
            fields_to_index={"str_field", "non_existing_field"},
        )

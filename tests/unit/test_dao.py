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

"""Testing the dao factory protocol."""

from collections.abc import Collection
from typing import Optional

import pytest

from hexkit.protocols.dao import BaseModelWithId, Dao, DaoFactoryProtocol, Dto

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


class ExampleDto(BaseModelWithId):
    """Example DTO model."""

    some_param: str
    another_param: int


async def test_get_dto_valid():
    """Use the get_dao method of the DaoFactory with valid parameters."""
    dao_factory = FakeDaoFactory()

    with pytest.raises(NotImplementedError):
        _ = await dao_factory.get_dao(
            name="test_dao",
            dto_model=ExampleDto,
            id_field="id",
            fields_to_index={"some_param"},
        )


async def test_get_dto_invalid_id():
    """Use the get_dao method of the DaoFactory with an invalid ID that is not found in
    the provided DTO model.
    """
    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtocol.IdFieldNotFoundError):
        _ = await dao_factory.get_dao(
            name="test_dao", dto_model=ExampleDto, id_field="invalid_id"
        )


async def test_get_dto_invalid_fields_to_index():
    """Use the get_dao method of the DaoFactory with an invalid list of fields to index."""
    dao_factory = FakeDaoFactory()

    with pytest.raises(DaoFactoryProtocol.IndexFieldsInvalidError):
        _ = await dao_factory.get_dao(
            name="test_dao",
            dto_model=ExampleDto,
            id_field="id",
            fields_to_index={"some_param", "non_existing_param"},
        )

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

"""A mock (in-memory) DAO"""

from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from copy import deepcopy
from typing import Any, Generic, TypeVar
from unittest.mock import AsyncMock, Mock

from pydantic import BaseModel

from hexkit.custom_types import ID
from hexkit.protocols.dao import (
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)

__all__ = ["MockDAOEmptyError", "new_mock_dao_class"]

DTO = TypeVar("DTO", bound=BaseModel)


class MockDAOEmptyError(RuntimeError):
    """Raised when attempting to access the `latest` property of an empty mock DAO"""


class BaseInMemDao(Generic[DTO]):
    """DAO with proper typing and in-memory storage for use in testing"""

    _id_field: str
    publish_pending = AsyncMock()
    republish = AsyncMock()
    with_transaction = Mock()

    def __init__(self) -> None:
        self.resources: dict[ID, DTO] = {}

    @property
    def latest(self) -> DTO:
        """Return the most recently inserted resource.

        Raises a MockDAOEmptyError if there are no resources stored.
        """
        try:
            return deepcopy(next(reversed(self.resources.values())))
        except StopIteration as err:
            raise MockDAOEmptyError() from err

    async def get_by_id(self, id_: ID) -> DTO:
        """Get the resource via ID.

        Raises a ResourceNotFoundError if no resource with a matching ID is found.
        """
        with suppress(KeyError):
            return deepcopy(self.resources[id_])
        raise ResourceNotFoundError(id_=id_)

    async def find_one(self, *, mapping: Mapping[str, Any]) -> DTO:
        """Find the resource that matches the specified mapping.

        Raises:
            NoHitsFoundError: If no matching resource is found.
            MultipleHitsFoundError: If more than one matching resource is found.
        """
        hits = self.find_all(mapping=mapping)
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

    async def find_all(self, *, mapping: Mapping[str, Any]) -> AsyncIterator[DTO]:
        """Find all resources that match the specified mapping."""
        for resource in self.resources.values():
            if all(getattr(resource, k) == v for k, v in mapping.items()):
                yield deepcopy(resource)

    async def insert(self, dto: DTO) -> None:
        """Insert a resource.

        Raises a ResourceAlreadyExistsError if a resource with a matching ID exists already.
        """
        dto_id = getattr(dto, self._id_field)
        if dto_id in self.resources:
            raise ResourceAlreadyExistsError(id_=dto_id)
        self.resources[dto_id] = deepcopy(dto)

    async def update(self, dto: DTO) -> None:
        """Update a resource.

        Raises a ResourceNotFoundError if no resource with a matching ID is found.
        """
        dto_id = getattr(dto, self._id_field)
        if dto_id not in self.resources:
            raise ResourceNotFoundError(id_=getattr(dto, self._id_field))
        self.resources[dto_id] = deepcopy(dto)

    async def delete(self, id_: ID) -> None:
        """Delete a resource by ID.

        Raises a ResourceNotFoundError if no resource with a matching ID is found.
        """
        if id_ not in self.resources:
            raise ResourceNotFoundError(id_=id_)
        del self.resources[id_]

    async def upsert(self, dto: DTO) -> None:
        """Upsert a resource."""
        dto_id = getattr(dto, self._id_field)
        self.resources[dto_id] = deepcopy(dto)


def new_mock_dao_class(
    *, dto_model: type[DTO], id_field: str
) -> type[BaseInMemDao[DTO]]:
    """Produce a mock DAO for the given DTO model and ID field"""

    class MockDao(BaseInMemDao[DTO]):
        """Mock dao that stores data in memory"""

        _id_field: str = id_field

    return MockDao

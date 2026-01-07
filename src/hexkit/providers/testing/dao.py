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

from collections.abc import AsyncIterator, Callable, Mapping
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

__all__ = ["SUPPORTED_MQL_OPERATORS", "MockDAOEmptyError", "new_mock_dao_class"]

DTO = TypeVar("DTO", bound=BaseModel)

SUPPORTED_MQL_OPERATORS = [
    "$eq",
    "$gt",
    "$gte",
    "$in",
    "$lt",
    "$lte",
    "$ne",
    "$nin",
]

MQL_COMPARISON_OP_MAP: dict[str, Callable[[Any, Any], bool]] = {
    "$eq": lambda a, b: a == b,
    "$gt": lambda a, b: a > b,
    "$gte": lambda a, b: a >= b,
    "$in": lambda a, b: a in b,
    "$lt": lambda a, b: a < b,
    "$lte": lambda a, b: a <= b,
    "$ne": lambda a, b: a != b,
    "$nin": lambda a, b: a not in b,
}


def _get_mql_predicates(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
    """Given a filter mapping, returns the sub-mappings that appear to be MQL predicates"""
    return {k: v for k, v in mapping.items() if k.lower() in SUPPORTED_MQL_OPERATORS}


def _ensure_spec_has_only_one_kv_pair(spec: Mapping[str, Any]):
    """Ensures that `spec` has only one key-value pair and raises an MQLError otherwise"""
    valid = len(spec) == 1
    if not valid:
        raise MQLError(f"Expected mapping {spec} to have only one key-value pair.")


class MockDAOEmptyError(RuntimeError):
    """Raised when attempting to access the `latest` property of an empty mock DAO"""


class MQLError(RuntimeError):
    """Raised when MQL parameters don't pass validation"""


class BaseInMemDao(Generic[DTO]):
    """DAO with proper typing and in-memory storage for use in testing"""

    _id_field: str
    _handle_mql: bool
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
            failed_predicates: dict[str, Any] = {}
            for k, v in mapping.items():
                try:
                    if getattr(resource, k) != v:
                        failed_predicates[k] = v
                except AttributeError:
                    failed_predicates[k] = v

            if not failed_predicates:
                yield deepcopy(resource)
            else:
                # If resource fails at least one predicate, conditionally re-inspect as mql
                mql_predicates = _get_mql_predicates(failed_predicates)
                if (
                    mql_predicates.keys() == failed_predicates.keys()
                    and self._handle_mql
                ):
                    for op, spec in mql_predicates.items():
                        if not self._resolve_mql_predicate(
                            resource=resource, op=op, spec=spec
                        ):
                            break
                    else:
                        yield deepcopy(resource)

    def _resolve_mql_predicate(
        self, *, resource: DTO, op: str, spec: Mapping[str, Any]
    ) -> bool:
        """Attempts to resolve MQL mapping and returns True if the resource satisfies
        the predicate and False otherwise.
        """
        op = op.lower()

        # Currently, this should always equate to True
        if op in MQL_COMPARISON_OP_MAP:
            _ensure_spec_has_only_one_kv_pair(spec)
            field, spec_value = {**spec}.popitem()
            resource_value = getattr(resource, field)
            return MQL_COMPARISON_OP_MAP[op](resource_value, spec_value)

        # This error block is only speculative right now
        raise MQLError(f"The {op} operator is not supported for use with the InMemDao.")

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
    *, dto_model: type[DTO], id_field: str, handle_mql: bool = True
) -> type[BaseInMemDao[DTO]]:
    """Produce a mock DAO for the given DTO model and ID field.

    If `handle_mql` is True, the DAO will attempt to resolve query mappings that
    use MongoDB query language predicates (e.g. $ne, $in, $ge, etc.). Not all MQL
    operators are supported. Please see `SUPPORTED_MQL_OPERATORS` for a complete list
    of the currently supported MQL operators.
    """

    class MockDao(BaseInMemDao[DTO]):
        """Mock dao that stores data in memory"""

        _id_field: str = id_field
        _handle_mql: bool = handle_mql

    return MockDao

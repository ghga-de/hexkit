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

from abc import ABC, abstractmethod
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

UNSUPPORTED_MQL_OPERATORS: set[str] = {
    "$all",
    "$elemMatch",
    "$size",
    "$bitsAllClear",
    "$bitsAllSet",
    "$bitsAnyClear",
    "$bitsAnySet",
    "$type",
    "$box",
    "$center",
    "$centerSphere",
    "$geolntersects",
    "$geometry",
    "$geoWithin",
    "$maxDistance",
    "$minDistance",
    "$near",
    "$nearSphere",
    "$polygon",
    "$expr",
    "$jsonSchema",
    "$mod",
    "$regex",
    "$where",
}

MQL_COMPARISON_OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "$eq": lambda a, b: a == b,
    "$gt": lambda a, b: a > b,
    "$gte": lambda a, b: a >= b,
    "$in": lambda a, b: a in b,
    "$lt": lambda a, b: a < b,
    "$lte": lambda a, b: a <= b,
    "$ne": lambda a, b: a != b,
    "$nin": lambda a, b: a not in b,
}

MQL_LOGICAL_OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "$and": lambda cs, v: all(c.evaluate(v) for c in cs),
    "$nor": lambda cs, v: not (any(c.evaluate(v) for c in cs)),
    "$not": lambda cs, v: not (any(c.evaluate(v) for c in cs)),
    "$or": lambda cs, v: any(c.evaluate(v) for c in cs),
}

MQL_DATA_TYPE_OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "$exists": lambda f, r: f in r
}

SUPPORTED_MQL_OPERATORS = (
    set(MQL_COMPARISON_OPERATORS)
    | set(MQL_LOGICAL_OPERATORS)
    | set(MQL_DATA_TYPE_OPERATORS)
)


class Predicate(ABC):
    @abstractmethod
    def evaluate(self, resource: dict[str, Any]) -> bool: ...


class ComparisonPredicate(Predicate):
    def __init__(
        self,
        *,
        op: str,
        field: str,
        target_value: Any | None = None,
        nested: list | None = None,
    ):
        self._op = op
        self._field = field
        self._target_value = target_value
        self._nested = nested

    def __repr__(self) -> str:
        return f"ComparisonPredicate(op='{self._op}',field='{self._field}',target_value={self._target_value!s},nested={self._nested})"

    def __eq__(self, other) -> bool:
        return (
            self._op == other._op
            and self._field == other._field
            and self._target_value == other._target_value
            and all(
                sn == on
                for sn, on in zip(self._nested or [], other._nested or [], strict=True)
            )
        )

    def evaluate(self, resource: dict[str, Any]) -> bool:
        if self._nested:
            for subp in self._nested:
                if not subp.evaluate(resource=resource[self._field]):
                    return False
            return True
        else:
            return MQL_COMPARISON_OPERATORS[self._op](
                resource.get(self._field), self._target_value
            )


def _build_comparison_predicate(
    *, op: str, field: str, mapping: Mapping[str, Any]
) -> ComparisonPredicate:
    if isinstance(mapping, dict):
        return ComparisonPredicate(op=op, field=field, nested=build_predicates(mapping))
    return ComparisonPredicate(op=op, field=field, target_value=mapping)


class LogicalPredicate(Predicate):
    def __init__(self, *, op: str, field: str, conditions: list[Predicate]):
        self._op = op
        self._field = field
        self._conditions = conditions

    def __repr__(self) -> str:
        return f"LogicalPredicate(op='{self._op}',field='{self._field}',conditions={self._conditions})"

    def __eq__(self, other) -> bool:
        return (
            self._op == other._op
            and self._field == other._field
            and all(
                sn == on
                for sn, on in zip(self._conditions, other._conditions, strict=True)
            )
        )

    def evaluate(self, resource: dict[str, Any]) -> bool:
        return MQL_LOGICAL_OPERATORS[self._op](self._conditions, resource)


def _build_logical_predicate(
    *, op: str, field: str, mapping: Mapping[str, Any] | list
) -> LogicalPredicate:
    if not isinstance(mapping, (list, dict)):
        raise MQLError(f"The mapping {mapping} is not valid for a LogicalPredicate")

    conditions = (
        build_predicates(mapping={field: mapping})
        if op == "$not"
        else build_predicates(
            {field: {k: v for condition in mapping for k, v in condition.items()}}
        )
    )
    return LogicalPredicate(op=op, field=field, conditions=conditions)


class DataTypePredicate(Predicate):
    def __init__(self, *, op: str, field: str):
        self._op = op
        self._field = field

    def __repr__(self) -> str:
        return f"DataTypePredicate(op='{self._op}',field='{self._field}')"

    def __eq__(self, other) -> bool:
        return self._op == other._op and self._field == other._field

    def evaluate(self, resource: dict[str, Any]) -> bool:
        return self._field in resource


def _build_mql_predicate(
    *, op: str, field: str, mapping: Mapping[str, Any] | list
) -> Predicate:
    if op in UNSUPPORTED_MQL_OPERATORS:
        raise MQLError(f"The {op} operator is not supported for use with the InMemDao.")
    if op in MQL_COMPARISON_OPERATORS:
        return _build_comparison_predicate(op=op, field=field, mapping=mapping)  # type: ignore
    elif op in MQL_LOGICAL_OPERATORS:
        return _build_logical_predicate(op=op, field=field, mapping=mapping)
    elif op in MQL_DATA_TYPE_OPERATORS:
        return DataTypePredicate(op=op, field=field)
    else:
        return ComparisonPredicate(op="$eq", field=field, target_value=mapping)


def build_predicates(mapping: Mapping[str, Any]) -> list[Predicate]:
    predicates: list[Predicate] = []
    for field, value in mapping.items():
        # If it's not a dict, assume that it's a direct equivalence predicate
        if not isinstance(value, dict) or set(value.keys()).isdisjoint(
            SUPPORTED_MQL_OPERATORS
        ):
            predicates.append(
                ComparisonPredicate(op="$eq", field=field, target_value=value)
            )
        else:
            for op, predv in value.items():
                predicates.append(
                    _build_mql_predicate(op=op.lower(), field=field, mapping=predv)
                )
    return predicates


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
        if self._handle_mql:
            predicates = build_predicates(mapping)
        for resource in self.resources.values():
            if self._handle_mql:
                if all(p.evaluate(resource=resource.model_dump()) for p in predicates):
                    yield deepcopy(resource)
            elif all(getattr(resource, k) == v for k, v in mapping.items()):
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

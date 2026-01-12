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
from typing import Any, Generic, Protocol, TypeVar
from unittest.mock import AsyncMock, Mock

from pydantic import BaseModel

from hexkit.custom_types import ID
from hexkit.protocols.dao import (
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from hexkit.providers.mongodb.provider import (
    document_to_dto,
    dto_to_document,
    replace_id_field_in_find_mapping,
)

__all__ = ["SUPPORTED_MQL_OPERATORS", "MockDAOEmptyError", "new_mock_dao_class"]

DTO = TypeVar("DTO", bound=BaseModel)
Document = dict[str, Any]

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


def _get_nested_value(resource: Document, field_path: str) -> Any:
    """Get a nested value from a resource using dot notation."""
    keys = field_path.split(".")
    if not all(keys):
        raise KeyError(f"Query key {field_path} uses improper dot notation")

    value: Document | None = resource
    for key in keys:
        if value is not None:
            try:
                value = value.get(key)
            except (KeyError, AttributeError):
                value = None
    return value


class Predicate(Protocol):
    def evaluate(self, resource: dict[str, Any]) -> bool:
        """Determine if the supplied dict value satisfies the predicate."""
        ...


class ComparisonPredicate(Predicate):
    """A Predicate that handles MQL comparison operators"""

    def __init__(
        self,
        *,
        op: str,
        field: str,
        target_value: Any | None,
    ):
        """Initialize the predicate.

        Parameters:
        - op denotes the operator string, e.g. '$eq', '$in', etc.
        - field indicates the name of the field in the data model that the condition applies to.
        - target_value is the second operand supplied to the eval function.
        """
        self._op = op
        try:
            self._fn = MQL_COMPARISON_OPERATORS[op]
        except KeyError as err:
            raise MQLError(
                f"{op!r} is not a supported Comparison MQL operator"
            ) from err

        if op in ["$in", "$nin"] and not isinstance(target_value, (list, tuple)):
            raise MQLError(
                f"The {op} operator must point to a list or tuple, not {type(target_value)}."
            )

        if not field:
            # field was None or "", probably a nesting error. Explain syntax.
            raise MQLError(
                f"The correct syntax for the {op!r} operator is "
                + f"{{<field name>: {{{op!r}: <expression>}}}}"
            )
        self._field = field
        self._target_value = target_value

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(op='{self._op}',field='{self._field}'"
            + f",target_value={self._target_value!s})"
        )

    def __eq__(self, other) -> bool:
        """Two ComparisonPredicates are equivalent if they share the same operator,
        field, and target value.
        """
        return (
            isinstance(other, ComparisonPredicate)
            and self._op == other._op
            and self._field == other._field
            and self._target_value == other._target_value
        )

    def evaluate(self, resource: dict[str, Any]) -> bool:
        """Determine if the supplied dict value satisfies the predicate.

        Dot notation is recognized and used to located the correct value.
        """
        value = _get_nested_value(resource, self._field)

        # Perform comparison on final value
        return self._fn(value, self._target_value)


class LogicalPredicate(Predicate):
    """A Predicate that handles MQL logical operators"""

    def __init__(
        self, *, op: str, field: str | None, mapping: Mapping[str, Any] | list
    ):
        """Initialize the predicate.

        Parameters:
        - op denotes the operator string, i.e. '$and', '$or', '$not', or '$nor'
        - conditions is a list of Predicates that constitute the logical predicate.
        """
        self._op = op
        try:
            self._fn = MQL_LOGICAL_OPERATORS[op]
        except KeyError as err:
            raise MQLError(f"{op!r} is not a supported Logical MQL operator") from err
        self._conditions: list[Predicate] = []

        if op == "$not":
            if not field:
                # field was None or "", probably a nesting error. Explain syntax.
                raise MQLError(
                    f"The correct syntax for the {op!r} operator is "
                    + f"{{<field name>: {{{op!r}: <expression>}}}}"
                )
            if not isinstance(mapping, dict) or len(mapping) != 1:
                raise MQLError(
                    "The $not expects a single dict with a single MQL operator key"
                )
            nextop = next(iter(mapping))
            if nextop in MQL_LOGICAL_OPERATORS and nextop != "$not":
                raise MQLError("Cannot nest logical operators under a $not operator.")
            if nextop not in SUPPORTED_MQL_OPERATORS:
                raise MQLError(
                    "The $not operator expects a dict with another MQL operator as the key."
                )
            self._conditions = build_predicates({field: mapping})
        else:
            if not isinstance(mapping, list) or len(mapping) == 0:
                raise MQLError(f"The {op} operator must be used with a non-empty list.")
            for condition in mapping:
                self._conditions.extend(build_predicates(mapping=condition))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(op='{self._op}',conditions={self._conditions})"
        )

    def __eq__(self, other) -> bool:
        """Two LogicalPredicates are equivalent if they share the same operator and
        all conditions are equivalent.
        """
        return (
            isinstance(other, LogicalPredicate)
            and self._op == other._op
            and all(
                sn == on
                for sn, on in zip(self._conditions, other._conditions, strict=True)
            )
        )

    def evaluate(self, resource: dict[str, Any]) -> bool:
        return self._fn(self._conditions, resource)


class DataTypePredicate(Predicate):
    """A Predicate that handles MQL data type operators - currently only $exists"""

    def __init__(self, *, op: str, field: str, target_value: bool):
        """Initialize the predicate.

        Parameters:
        - op denotes the operator string, i.e. '$exists'
        - field indicates the name of the field in the data model that the condition applies to.
        - target_value is either True or False
        """
        self._op = op
        try:
            self._fn = MQL_DATA_TYPE_OPERATORS[op]
        except KeyError as err:
            raise MQLError(f"{op!r} is not a supported Data Type MQL operator") from err

        if not field:
            # field was None or "", probably a nesting error. Explain syntax.
            raise MQLError(
                f"The correct syntax for the {op!r} operator is "
                + f"{{<field name>: {{{op!r}: <expression>}}}}"
            )

        if not isinstance(target_value, bool):
            raise MQLError(
                "The $exists operator must point to a boolean. It is highly"
                + " recommend to use an explicit boolean."
            )

        self._field = field
        self._target_value = target_value

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(op='{self._op}',field='{self._field}'"
            + f",target_value={self._target_value!s})"
        )

    def __eq__(self, other) -> bool:
        """Two DataTypePredicates are the same if they have the same operator, field
        and target value.
        """
        return (
            isinstance(other, DataTypePredicate)
            and self._op == other._op
            and self._field == other._field
            and self._target_value == other._target_value
        )

    def evaluate(self, resource: dict[str, Any]) -> bool:
        if "." in self._field:
            path = self._field.rsplit(".", 1)
            value = _get_nested_value(resource, path[0])
            return self._fn(path[1], value) == self._target_value
        else:
            return self._fn(self._field, resource) == self._target_value


def _build_mql_predicate(*, op: str, field: str | None, mapping: Any) -> Predicate:
    """Given an MQL operator, field name, and mapping, construct a Predicate instance.

    Raises an MQLError if there is a problem with the operands or structure.
    """
    if op in MQL_COMPARISON_OPERATORS:
        return ComparisonPredicate(
            op=op,
            field=field,  # type: ignore
            target_value=mapping,
        )
    elif op in MQL_LOGICAL_OPERATORS:
        return LogicalPredicate(op=op, field=field, mapping=mapping)
    elif op in MQL_DATA_TYPE_OPERATORS:
        return DataTypePredicate(op=op, field=field, target_value=mapping)  # type: ignore
    else:
        raise MQLError(f"The {op} operator is not supported for use with the InMemDao.")


def build_predicates(mapping: Mapping[str, Any]) -> list[Predicate]:
    """Build a list of Predicates from a search filter mapping."""
    predicates: list[Predicate] = []
    for key, value in mapping.items():
        # If the key is an operator (likely $and, $not, or $or), build predicate
        if (op := key.lower()).startswith("$"):
            predicates.append(_build_mql_predicate(op=op, field=None, mapping=value))
        # If it's not a dict OR it's a dict but none of the keys are MQL operators,
        #  assume that this k-v pair says field 'key' needs to equal the object 'value'
        elif not isinstance(value, dict) or set(value).isdisjoint(
            SUPPORTED_MQL_OPERATORS
        ):
            predicates.append(
                ComparisonPredicate(op="$eq", field=key, target_value=value)
            )
        # If key is a field name, the value is a dict, and the dict has at least one op,
        #  build a predicate object for each item in the dictionary
        else:
            for op, predv in value.items():
                predicates.append(
                    _build_mql_predicate(op=op.lower(), field=key, mapping=predv)
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
    _serialize: Callable
    _deserialize: Callable
    publish_pending = AsyncMock()
    republish = AsyncMock()
    with_transaction = Mock()

    def __init__(self) -> None:
        self.resources: dict[ID, Document] = {}

    @property
    def latest(self) -> DTO:
        """Return the most recently inserted resource.

        Raises a MockDAOEmptyError if there are no resources stored.
        """
        try:
            return self._deserialize(next(reversed(self.resources.values())))
        except StopIteration as err:
            raise MockDAOEmptyError() from err

    async def get_by_id(self, id_: ID) -> DTO:
        """Get the resource via ID.

        Raises a ResourceNotFoundError if no resource with a matching ID is found.
        """
        with suppress(KeyError):
            return self._deserialize(self.resources[id_])
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
        if "" in mapping:
            raise KeyError("Query mappings can't contain empty-string keys")

        _mapping = replace_id_field_in_find_mapping(mapping, self._id_field)
        predicates = build_predicates(_mapping) if self._handle_mql else []

        for resource in self.resources.values():
            if self._handle_mql:
                matches = all(p.evaluate(resource=resource) for p in predicates)
            else:
                matches = True
                for key, expected_value in mapping.items():
                    actual_value = _get_nested_value(resource, key)
                    if actual_value != expected_value:
                        matches = False
                        break
            if matches:
                yield self._deserialize(resource)

    async def insert(self, dto: DTO) -> None:
        """Insert a resource.

        Raises a ResourceAlreadyExistsError if a resource with a matching ID exists already.
        """
        dto_id = getattr(dto, self._id_field)
        if dto_id in self.resources:
            raise ResourceAlreadyExistsError(id_=dto_id)
        self.resources[dto_id] = self._serialize(dto)

    async def update(self, dto: DTO) -> None:
        """Update a resource.

        Raises a ResourceNotFoundError if no resource with a matching ID is found.
        """
        dto_id = getattr(dto, self._id_field)
        if dto_id not in self.resources:
            raise ResourceNotFoundError(id_=getattr(dto, self._id_field))
        self.resources[dto_id] = self._serialize(dto)

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
        self.resources[dto_id] = self._serialize(dto)


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

        def _serialize(self, dto: DTO) -> Document:
            return dto_to_document(dto, id_field=id_field)

        def _deserialize(self, document: Document) -> DTO:
            return document_to_dto(
                deepcopy(document), id_field=id_field, dto_model=dto_model
            )

    return MockDao

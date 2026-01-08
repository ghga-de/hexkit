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

"""Testing the in-memory DAO."""

from typing import Any

import pytest
from pydantic import BaseModel

from hexkit.protocols.dao import NoHitsFoundError, ResourceNotFoundError
from hexkit.providers.testing import MockDAOEmptyError, new_mock_dao_class
from hexkit.providers.testing.dao import (
    ComparisonPredicate,
    DataTypePredicate,
    LogicalPredicate,
    MQLError,
    Predicate,
    build_predicates,
)

pytestmark = pytest.mark.asyncio()


class InventoryItem(BaseModel):
    """A model that can be used for testing"""

    title: str
    count: int


DaoClass = new_mock_dao_class(dto_model=InventoryItem, id_field="title")


async def test_latest_while_empty():
    """Test the MockDAO testing utilities."""
    dao = DaoClass()
    with pytest.raises(MockDAOEmptyError):
        _ = dao.latest


async def test_insertion():
    """Test the `insert()` method"""
    dao = DaoClass()
    item = InventoryItem(title="Wrench", count=12)

    await dao.insert(item)
    assert dao.latest is not item  # verify that .latest returns a copy
    assert dao.latest.model_dump() == item.model_dump()


async def test_deletion():
    """Test the `delete()` method"""
    dao = DaoClass()
    with pytest.raises(ResourceNotFoundError):
        await dao.delete("doesnotexist")

    # Insert item and delete it
    item = InventoryItem(title="Candle", count=100)
    await dao.insert(item)
    assert dao.latest.model_dump() == item.model_dump()
    await dao.delete("Candle")

    with pytest.raises(MockDAOEmptyError):
        _ = dao.latest

    with pytest.raises(NoHitsFoundError):
        _ = await dao.find_one(mapping={})


async def test_update():
    """Test the `update()` method"""
    dao = DaoClass()
    item = InventoryItem(title="Nudelholz", count=1)

    # Updating a non-existent item raises an error
    with pytest.raises(ResourceNotFoundError):
        await dao.update(item)

    # Insert the item and then update
    await dao.insert(item)
    item_update = item.model_copy(update={"count": 2})

    await dao.update(item_update)
    assert dao.latest.count == 2


async def test_upsert():
    """Test the `upsert()` method"""
    dao = DaoClass()
    item = InventoryItem(title="Nudelholz", count=1)

    # Upserting a non-existent item DOES NOT raise an error
    await dao.upsert(item)
    assert dao.latest.model_dump() == item.model_dump()

    # Upsert the original item by changing `count`
    await dao.upsert(item.model_copy(update={"count": 2}))
    assert dao.latest.count == 2


async def test_find_one():
    """Test the `find_one()` method"""
    dao = DaoClass()

    with pytest.raises(NoHitsFoundError):
        await dao.find_one(mapping={"title": "Lawnmower"})

    item = InventoryItem(title="Lawnmower", count=9)
    await dao.insert(item)

    result = await dao.find_one(mapping={"title": "Lawnmower"})
    assert result is not item
    assert result.model_dump() == item.model_dump()


async def test_find_all():
    """Test the `find_all()` method"""
    dao = DaoClass()
    brick = InventoryItem(title="Brick", count=1)
    shovel = InventoryItem(title="Shovel", count=1)
    bat = InventoryItem(title="Bat", count=2)
    await dao.insert(brick)
    await dao.insert(shovel)
    await dao.insert(bat)

    # Get resources with count=1, sorted alphabetically by title
    results = sorted(
        [x async for x in dao.find_all(mapping={"count": 1})], key=lambda x: x.title
    )
    assert results
    assert len(results) == 2
    assert results[0].title == "Brick"
    assert results[1].title == "Shovel"

    # Filter by the id field
    results = [x async for x in dao.find_all(mapping={"title": "Bat"})]
    assert results
    assert results[0] is not bat and (bat.model_dump() == results[0].model_dump())

    # Look for something that doesn't exist
    results = [x async for x in dao.find_all(mapping={"title": "Broom"})]
    assert not results

    # Get everything
    results = sorted([x async for x in dao.find_all(mapping={})], key=lambda x: x.title)
    assert len(results) == 3
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [bat, brick, shovel]
    ]


async def test_get_by_id():
    """Test the `get_by_id()` method"""
    dao = DaoClass()

    # Try with an ID that doesn't exist -- should raise an error
    with pytest.raises(ResourceNotFoundError):
        _ = await dao.get_by_id("Pumpkin")

    # Insert an item
    that_pumpkin = InventoryItem(title="Pumpkin", count=4000)
    await dao.insert(that_pumpkin)

    # Look for the item with get_by_id
    this_pumpkin = await dao.get_by_id("Pumpkin")
    assert this_pumpkin is not that_pumpkin
    assert this_pumpkin.model_dump() == that_pumpkin.model_dump()


async def test_mql_comparison_ops():
    """Test the different MQL comparison operators"""
    dao = DaoClass()
    brick = InventoryItem(title="Brick", count=1)
    shovel = InventoryItem(title="Shovel", count=2)
    tophat = InventoryItem(title="Tophat", count=3)
    await dao.insert(brick)
    await dao.insert(shovel)
    await dao.insert(tophat)

    mapping: dict[str, Any] = {"title": {"$eq": "Brick"}}
    results = [x async for x in dao.find_all(mapping=mapping)]
    assert len(results) == 1
    assert results[0].model_dump() == brick.model_dump()

    mapping = {"count": {"$gt": 1}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 2
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [shovel, tophat]
    ]

    mapping = {"count": {"$gte": 1}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 3
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel, tophat]
    ]

    mapping = {"title": {"$in": ["Brick", "Shovel"]}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 2
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel]
    ]

    mapping = {"count": {"$lt": 3}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 2
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel]
    ]

    mapping = {"count": {"$lte": 3}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 3
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel, tophat]
    ]

    mapping = {"title": {"$ne": "Tophat"}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 2
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel]
    ]

    mapping = {"title": {"$nin": "This is my Tophat".split()}}
    results = sorted(
        [x async for x in dao.find_all(mapping=mapping)], key=lambda x: x.title
    )
    assert len(results) == 2
    assert [x.model_dump() for x in results] == [
        x.model_dump() for x in [brick, shovel]
    ]


def test_build_predicates_empty_mapping():
    """Verify that running build_predicates() on an empty dict returns an empty list"""
    assert build_predicates(dict()) == []


def test_basic_nesting():
    """Verify nesting interpretation in predicate building"""
    mapping = {
        "field1": {"$eq": {"fieldX": 123}},
        "field1.fieldY": {"$gt": 9001},
    }
    expected_predicates = [
        ComparisonPredicate(op="$eq", field="field1", target_value={"fieldX": 123}),
        ComparisonPredicate(op="$gt", field="field1.fieldY", target_value=9001),
    ]
    assert build_predicates(mapping) == expected_predicates

    # No error here because MongoDB interprets {"$gt": 9001} literally
    mapping = {
        "field1": {
            "$eq": {
                "fieldX": 123,
                "fieldY": {"$gt": 9001},
            }
        }
    }
    expected_predicates = [
        ComparisonPredicate(
            op="$eq",
            field="field1",
            target_value={"fieldX": 123, "fieldY": {"$gt": 9001}},
        ),
    ]


@pytest.mark.parametrize("op", ["$eq", "$ne"])
def test_eq_ne(op: str):
    """Test the $eq and $ne operators in predicate building"""
    # Standard, proper usage
    for value in [17, False, ["foo", "bar"], {}, {"$gt": "y"}, b"baz", None]:
        mapping = {"my_field": {op: value}}
        expected_predicates = [
            ComparisonPredicate(op=op, field="my_field", target_value=value)
        ]
        assert build_predicates(mapping) == expected_predicates

    # Inverted nesting
    with pytest.raises(MQLError):
        _ = build_predicates({op: {"age": [32, 66, 83]}})


@pytest.mark.parametrize("op", ["$gt", "$lt", "$gte", "$lte"])
def test_gt_lt_gte_lte(op: str):
    """Test the $gt, $lt, $gte, and $lte operators in predicate building"""
    mapping = {"count": {op: 0}}
    expected_predicates = [ComparisonPredicate(op=op, field="count", target_value=0)]
    assert build_predicates(mapping) == expected_predicates


@pytest.mark.parametrize("op", ["$in", "$nin"])
def test_in_nin(op: str):
    """Test the $in and $nin operators in predicate building"""
    # Standard, proper usage, including empty values
    arrays = [[1, 2, 3], (1, 2, 3), [], ()]
    for arr in arrays:
        mapping = {"count": {op: arr}}
        assert build_predicates(mapping) == [
            ComparisonPredicate(op=op, field="count", target_value=arr)
        ]

    # Non-list values
    for nonlist_value in ["abc", {1, 2, 3}, {"some": "dict"}]:
        with pytest.raises(MQLError):
            _ = build_predicates({"f1": {op: nonlist_value}})

    # Inverted nesting
    with pytest.raises(MQLError):
        _ = build_predicates({op: {"age": [32, 66, 83]}})


def test_exists():
    """Test the $exists operator in predicate building"""
    for value in [True, False]:
        mapping = {"new_field": {"$exists": value}}
        assert build_predicates(mapping) == [
            DataTypePredicate(op="$exists", field="new_field", target_value=value)
        ]

    # Inverted nesting
    with pytest.raises(MQLError):
        _ = build_predicates({"$exists": {"new_field": value}})

    # Non-boolean values
    for bad_value in [0, 1, "True", {"set", "of", "strs"}, [9, 8, 7], b"bytestring"]:
        with pytest.raises(MQLError):
            _ = build_predicates({"new_field": {"$exists": bad_value}})


@pytest.mark.parametrize("op", ["$and", "$nor", "$or"])
def test_and(op: str):
    """Test the $and operator"""
    # Standard, proper usage
    mapping: dict[str, Any] = {
        op: [
            {"field1": {"$gt": 9001}},
            {"field2": {"$eq": ["some", "list", "values"]}},
        ]
    }
    expected_predicates: list[Predicate] = [
        LogicalPredicate(
            op=op,
            conditions=[
                ComparisonPredicate(op="$gt", field="field1", target_value=9001),
                ComparisonPredicate(
                    op="$eq", field="field2", target_value=["some", "list", "values"]
                ),
            ],
        )
    ]
    result = build_predicates(mapping)
    assert result == expected_predicates

    # Empty list
    with pytest.raises(MQLError):
        _ = build_predicates({op: []})

    # Does not use a list
    with pytest.raises(MQLError):
        _ = build_predicates({op: True})

    # Inverted nesting
    with pytest.raises(MQLError):
        _ = build_predicates({"field1": {op: [{"$lt": 99}]}})

    # Only one condition
    expected_predicates = [
        LogicalPredicate(
            op=op,
            conditions=[ComparisonPredicate(op="$eq", field="field", target_value=1)],
        )
    ]
    mapping = {op: [{"field": {"$eq": 1}}]}
    assert build_predicates(mapping) == expected_predicates

    # Demonstration of implicit $and (produces list of ComparisonPredicate)
    mapping = {"count": {"$gt": 1, "$lt": 10}}
    expected_predicates = [
        ComparisonPredicate(op="$gt", field="count", target_value=1),
        ComparisonPredicate(op="$lt", field="count", target_value=10),
    ]
    build_predicates(mapping)


def test_not():
    """Test the $not operator"""
    mapping: dict[str, Any] = {"field1": {"$not": {"$eq": 0}}}
    expected_predicates = [
        LogicalPredicate(
            op="$not",
            conditions=[ComparisonPredicate(op="$eq", field="field1", target_value=0)],
        )
    ]
    assert build_predicates(mapping) == expected_predicates

    # Does not contain an expression
    with pytest.raises(MQLError):
        _ = build_predicates({"field2": {"$not": {}}})

    # Does not point to an operator expression
    with pytest.raises(MQLError):
        _ = build_predicates({"field3": {"$not": {"k": "v"}}})

    # Not nested within a field
    with pytest.raises(MQLError):
        _ = build_predicates({"$not": {"field4": {"$eq": 0}}})

    # Does not point to a dict
    with pytest.raises(MQLError):
        _ = build_predicates({"field5": {"$not": ["$eq", 123]}})

    # Nesting is inverted
    with pytest.raises(MQLError):
        _ = build_predicates({"$not": {"field6": {"$eq": True}}})

    # Points to other logical operator aside from another $not
    for logop in ["$and", "$nor", "$or"]:
        mapping = {"count": {"$not": {logop: [{"$exists": True}, {"$gt": "5"}]}}}
        with pytest.raises(MQLError):
            _ = build_predicates(mapping)

    # Nested $nots
    mapping = {"verdict": {"$not": {"$not": {"$not": {"$ne": "guilty"}}}}}
    expected_predicates = [
        LogicalPredicate(
            op="$not",
            conditions=[
                LogicalPredicate(
                    op="$not",
                    conditions=[
                        LogicalPredicate(
                            op="$not",
                            conditions=[
                                ComparisonPredicate(
                                    op="$ne", field="verdict", target_value="guilty"
                                )
                            ],
                        )
                    ],
                )
            ],
        )
    ]
    assert build_predicates(mapping) == expected_predicates


def test_build_predicates_complex():
    """Test query predicate parsing for a mapping that uses nested logical operators"""
    mapping = {
        "$and": [
            {"count": {"$exists": True}},
            {
                "$or": [
                    {"count": {"$in": [11, 17, 81]}},
                    {"count": {"$eq": -1}},
                    {"count": {"$eq": -24}},
                    {"count": {"$gt": 100}},
                ]
            },
            {"count": {"$not": {"$gt": 150}}},
            {
                "$nor": [
                    {"animal": {"$eq": "sheep"}},
                    {"animal": {"$exists": False}},
                    {"age": {"$gt": 5}},
                    {"age": {"$lt": 10}},
                    {"some_dict": {"k": "v"}},
                ]
            },
        ],
        "data": {"some_bool": True},
    }
    expected_predicates = [
        LogicalPredicate(
            op="$and",
            conditions=[
                DataTypePredicate(op="$exists", field="count", target_value=True),
                LogicalPredicate(
                    op="$or",
                    conditions=[
                        ComparisonPredicate(
                            op="$in", field="count", target_value=[11, 17, 81]
                        ),
                        ComparisonPredicate(op="$eq", field="count", target_value=-1),
                        ComparisonPredicate(op="$eq", field="count", target_value=-24),
                        ComparisonPredicate(op="$gt", field="count", target_value=100),
                    ],
                ),
                LogicalPredicate(
                    op="$not",
                    conditions=[
                        ComparisonPredicate(op="$gt", field="count", target_value=150)
                    ],
                ),
                LogicalPredicate(
                    op="$nor",
                    conditions=[
                        ComparisonPredicate(
                            op="$eq", field="animal", target_value="sheep"
                        ),
                        DataTypePredicate(
                            op="$exists", field="animal", target_value=False
                        ),
                        ComparisonPredicate(op="$gt", field="age", target_value=5),
                        ComparisonPredicate(op="$lt", field="age", target_value=10),
                        ComparisonPredicate(
                            op="$eq", field="some_dict", target_value={"k": "v"}
                        ),
                    ],
                ),
            ],
        ),
        ComparisonPredicate(op="$eq", field="data", target_value={"some_bool": True}),
    ]

    assert build_predicates(mapping) == expected_predicates

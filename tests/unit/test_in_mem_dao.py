# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
    results = sorted([x.title async for x in dao.find_all(mapping={"count": 1})])
    assert results == ["Brick", "Shovel"]

    # Filter by the id field
    results = [x.title async for x in dao.find_all(mapping={"title": "Bat"})]
    assert results == ["Bat"]

    # Look for something that doesn't exist
    results = [x.title async for x in dao.find_all(mapping={"title": "Broom"})]
    assert not results

    # Get everything
    results = sorted([x.title async for x in dao.find_all(mapping={})])
    assert results == ["Bat", "Brick", "Shovel"]


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


@pytest.mark.parametrize(
    "mapping,expected",
    [
        ({"title": {"$eq": "Brick"}}, ["Brick"]),
        ({"count": {"$gt": 1}}, ["Shovel", "Tophat"]),
        ({"count": {"$gte": 1}}, ["Brick", "Shovel", "Tophat"]),
        ({"title": {"$in": ["Brick", "Shovel"]}}, ["Brick", "Shovel"]),
        ({"count": {"$lt": 3}}, ["Brick", "Shovel"]),
        ({"count": {"$lte": 3}}, ["Brick", "Shovel", "Tophat"]),
        ({"title": {"$ne": "Tophat"}}, ["Brick", "Shovel"]),
        ({"title": {"$nin": ["This", "is", "my", "Tophat"]}}, ["Brick", "Shovel"]),
    ],
)
async def test_mql_comparison_ops(mapping: dict[str, Any], expected: list[str]):
    """Test the different MQL comparison operators"""
    dao = DaoClass()
    brick = InventoryItem(title="Brick", count=1)
    shovel = InventoryItem(title="Shovel", count=2)
    tophat = InventoryItem(title="Tophat", count=3)
    await dao.insert(brick)
    await dao.insert(shovel)
    await dao.insert(tophat)

    results = sorted([x.title async for x in dao.find_all(mapping=mapping)])
    assert results == expected


async def test_build_predicates_empty_mapping():
    """Verify that running build_predicates() on an empty dict returns an empty list"""
    assert build_predicates(dict()) == []


async def test_basic_nesting():
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
async def test_eq_ne(op: str):
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
async def test_gt_lt_gte_lte(op: str):
    """Test the $gt, $lt, $gte, and $lte operators in predicate building"""
    mapping = {"count": {op: 0}}
    expected_predicates = [ComparisonPredicate(op=op, field="count", target_value=0)]
    assert build_predicates(mapping) == expected_predicates


@pytest.mark.parametrize("op", ["$in", "$nin"])
async def test_in_nin(op: str):
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


async def test_exists():
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
async def test_and(op: str):
    """Test the $and operator"""
    # Standard, proper usage
    mapping: dict[str, Any] = {
        op: [
            {"field1": {"$gt": 9001}},
            {"field2": {"$eq": ["some", "list", "values"]}},
        ]
    }
    expected_predicates: list[Predicate] = [
        LogicalPredicate(op=op, field=None, mapping=mapping[op])
    ]
    results = build_predicates(mapping)
    assert results == expected_predicates
    assert results[0]._conditions == [  # type: ignore
        ComparisonPredicate(op="$gt", field="field1", target_value=9001),
        ComparisonPredicate(
            op="$eq", field="field2", target_value=["some", "list", "values"]
        ),
    ]

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
    mapping = {op: [{"field": {"$eq": 1}}]}
    expected_predicates = [LogicalPredicate(op=op, field=None, mapping=mapping[op])]
    results = build_predicates(mapping)
    assert results == expected_predicates
    assert results[0]._conditions == [  # type: ignore
        ComparisonPredicate(op="$eq", field="field", target_value=1)
    ]

    # Demonstration of implicit $and (produces list of ComparisonPredicate)
    mapping = {"count": {"$gt": 1, "$lt": 10}}
    expected_predicates = [
        ComparisonPredicate(op="$gt", field="count", target_value=1),
        ComparisonPredicate(op="$lt", field="count", target_value=10),
    ]
    build_predicates(mapping)


async def test_not():
    """Test the $not operator"""
    mapping: dict[str, Any] = {"field1": {"$not": {"$eq": 0}}}
    expected_predicates = [
        LogicalPredicate(op="$not", field="field1", mapping={"$eq": 0})
    ]
    results = build_predicates(mapping)
    assert results == expected_predicates
    assert results[0]._conditions == [  # type: ignore
        ComparisonPredicate(op="$eq", field="field1", target_value=0)
    ]
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
            op="$not", field="verdict", mapping={"$not": {"$not": {"$ne": "guilty"}}}
        )
    ]

    results = build_predicates(mapping)
    assert results == expected_predicates
    assert isinstance(results[0]._conditions[0], LogicalPredicate)  # type: ignore
    assert isinstance(results[0]._conditions[0]._conditions[0], LogicalPredicate)  # type: ignore
    assert results[0]._conditions[0]._conditions[0]._conditions == [  # type: ignore
        ComparisonPredicate(op="$ne", field="verdict", target_value="guilty")
    ]


async def test_build_predicates_complex():
    """Test query predicate parsing for a mapping that uses nested logical operators"""
    mapping: dict[str, Any] = {
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
        LogicalPredicate(op="$and", field=None, mapping=mapping["$and"]),
        ComparisonPredicate(op="$eq", field="data", target_value={"some_bool": True}),
    ]

    results = build_predicates(mapping)
    assert results == expected_predicates

    # Verify the $and operator's conditions
    and_predicate = results[0]
    assert len(and_predicate._conditions) == 4  # type: ignore

    # First condition: {"count": {"$exists": True}}
    assert and_predicate._conditions[0] == DataTypePredicate(  # type: ignore
        op="$exists", field="count", target_value=True
    )

    # Second condition: $or operator
    or_predicate = and_predicate._conditions[1]  # type: ignore
    assert isinstance(or_predicate, LogicalPredicate)
    assert or_predicate._op == "$or"
    assert len(or_predicate._conditions) == 4
    assert or_predicate._conditions == [
        ComparisonPredicate(op="$in", field="count", target_value=[11, 17, 81]),
        ComparisonPredicate(op="$eq", field="count", target_value=-1),
        ComparisonPredicate(op="$eq", field="count", target_value=-24),
        ComparisonPredicate(op="$gt", field="count", target_value=100),
    ]

    # Third condition: $not operator
    not_predicate = and_predicate._conditions[2]  # type: ignore
    assert isinstance(not_predicate, LogicalPredicate)
    assert not_predicate._op == "$not"
    assert not_predicate._conditions == [
        ComparisonPredicate(op="$gt", field="count", target_value=150)
    ]

    # Fourth condition: $nor operator
    nor_predicate = and_predicate._conditions[3]  # type: ignore
    assert isinstance(nor_predicate, LogicalPredicate)
    assert nor_predicate._op == "$nor"
    assert len(nor_predicate._conditions) == 5
    assert nor_predicate._conditions == [
        ComparisonPredicate(op="$eq", field="animal", target_value="sheep"),
        DataTypePredicate(op="$exists", field="animal", target_value=False),
        ComparisonPredicate(op="$gt", field="age", target_value=5),
        ComparisonPredicate(op="$lt", field="age", target_value=10),
        ComparisonPredicate(op="$eq", field="some_dict", target_value={"k": "v"}),
    ]


async def test_fields_with_dollar_sign():
    """Verify that using a field name that starts with a dollar sign produces an error."""
    mapping = {"$bogus": 8}
    with pytest.raises(MQLError):
        _ = build_predicates(mapping)


async def test_find_all_sort():
    """Test find_all() with the sort parameter."""
    dao = DaoClass()
    await dao.insert(InventoryItem(title="Banana", count=2))
    await dao.insert(InventoryItem(title="Apple", count=3))
    await dao.insert(InventoryItem(title="Cherry", count=1))

    # sort ascending by title
    results = [x.title async for x in dao.find_all(mapping={}, sort=["title"])]
    assert results == ["Apple", "Banana", "Cherry"]

    # sort descending by title
    results = [x.title async for x in dao.find_all(mapping={}, sort=["-title"])]
    assert results == ["Cherry", "Banana", "Apple"]

    # sort ascending by count
    results = [x.title async for x in dao.find_all(mapping={}, sort=["count"])]
    assert results == ["Cherry", "Banana", "Apple"]


async def test_find_all_pagination():
    """Test find_all() with skip and/or limit (with sort for deterministic ordering)."""
    dao = DaoClass()
    titles = ["Apple", "Banana", "Cherry", "Date", "Elderberry"]
    for title in titles:
        await dao.insert(InventoryItem(title=title, count=1))

    asc_sort = ["title"]

    # skip=2 returns items starting from index 2
    results = [x.title async for x in dao.find_all(mapping={}, skip=2, sort=asc_sort)]
    assert results == ["Cherry", "Date", "Elderberry"]

    # limit=3 returns first 3 items
    results = [x.title async for x in dao.find_all(mapping={}, limit=3, sort=asc_sort)]
    assert results == ["Apple", "Banana", "Cherry"]

    # skip=1, limit=2 returns the window [1, 3)
    results = [
        x.title async for x in dao.find_all(mapping={}, skip=1, limit=2, sort=asc_sort)
    ]
    assert results == ["Banana", "Cherry"]

    # skip larger than collection size returns no results
    results = [x.title async for x in dao.find_all(mapping={}, skip=10, sort=asc_sort)]
    assert results == []


async def test_find_all_limit_zero_and_none():
    """Test limit=0 and limit=None, but that total_count works either way.

    When limit=0, no results should be returned.
    When limit=None, all results should be returned.
    """
    dao = DaoClass()
    titles = {"Apple", "Banana", "Cherry"}
    for title in titles:
        await dao.insert(InventoryItem(title=title, count=1))

    result = dao.find_all(mapping={}, limit=0)
    page = {x.title async for x in result}
    assert page == set()
    assert await result.total_count() == 3

    result = dao.find_all(mapping={}, limit=None)
    page = {x.title async for x in result}
    assert page == titles
    assert await result.total_count() == 3


async def test_find_all_pagination_empty_collection():
    """Test find_all() with skip and limit on an empty collection produces no errors."""
    dao = DaoClass()
    assert await dao.find_all(mapping={}, skip=5, limit=10).to_list() == []


async def test_find_all_pagination_negative_values():
    """Test find_all() raises ValueError when skip or limit are negative."""
    dao = DaoClass()

    with pytest.raises(ValueError):
        _ = dao.find_all(mapping={}, skip=-1)

    with pytest.raises(ValueError):
        _ = dao.find_all(mapping={}, limit=-1)

    with pytest.raises(ValueError):
        _ = dao.find_all(mapping={}, skip=-1, limit=-1)


async def test_find_all_sort_by_id_field():
    """Test that "_id" is used when sorting by the ID field.

    DaoClass uses 'title' as its id_field. Documents are stored with '_id' as
    the key internally, so sort=[("title", ...)] must translate to sorting on '_id'.
    """
    dao = DaoClass()
    await dao.insert(InventoryItem(title="Cherry", count=1))
    await dao.insert(InventoryItem(title="Apple", count=2))
    await dao.insert(InventoryItem(title="Banana", count=3))

    results = [x.title async for x in dao.find_all(mapping={}, sort=["title"])]
    assert results == ["Apple", "Banana", "Cherry"]

    results = [x.title async for x in dao.find_all(mapping={}, sort=["-title"])]
    assert results == ["Cherry", "Banana", "Apple"]


async def test_find_all_sort_compound():
    """Ensure compound sort (multiple fields) produces correct stable ordering."""
    dao = DaoClass()
    await dao.insert(InventoryItem(title="Cherry", count=1))
    await dao.insert(InventoryItem(title="Banana", count=2))
    await dao.insert(InventoryItem(title="Apple", count=2))
    await dao.insert(InventoryItem(title="Date", count=2))

    # Primary: count asc; secondary: title asc (tiebreak within count=2 group)
    results = [x.title async for x in dao.find_all(mapping={}, sort=["count", "title"])]
    assert results == ["Cherry", "Apple", "Banana", "Date"]


async def test_find_all_total_count_with_filter():
    """Make sure that total_count() reflects the filtered match count, not
    the total collection size.
    """
    dao = DaoClass()
    for title in ["Apple", "Banana", "Cherry"]:
        await dao.insert(InventoryItem(title=title, count=1))
    for title in ["Date", "Elderberry"]:
        await dao.insert(InventoryItem(title=title, count=2))

    result = dao.find_all(mapping={"count": 1}, skip=1, limit=1, sort=["title"])
    page = [x.title async for x in result]
    assert page == ["Banana"]

    total = await result.total_count()
    assert total == 3  # 3 match the filter, not 5


async def test_find_all_total_count_before_iteration():
    """Verify total_count() works correctly when called before consuming any results."""
    dao = DaoClass()
    for title in ["Apple", "Banana", "Cherry"]:
        await dao.insert(InventoryItem(title=title, count=1))

    result = dao.find_all(mapping={}, skip=1, limit=1)
    total = await result.total_count()
    assert total == 3

    page = await result.to_list()
    assert len(page) == 1


async def test_find_all_total_count_empty_filter_result():
    """Test that total_count() returns 0 when no documents match the mapping."""
    dao = DaoClass()
    await dao.insert(InventoryItem(title="Apple", count=1))
    await dao.insert(InventoryItem(title="Banana", count=1))

    result = dao.find_all(mapping={"count": 999})
    page = await result.to_list()
    assert page == []

    total = await result.total_count()
    assert total == 0


async def test_find_all_total_count():
    """Test that total_count() returns the full matching count regardless of skip/limit."""
    dao = DaoClass()
    titles = ["Apple", "Banana", "Cherry", "Date", "Elderberry"]
    for title in titles:
        await dao.insert(InventoryItem(title=title, count=1))

    result = dao.find_all(mapping={}, skip=2, limit=2, sort=["title"])
    page = [x.title async for x in result]
    assert page == ["Cherry", "Date"]

    total = await result.total_count()
    assert total == 5

    # calling total_count() a second time uses the cached value
    total_again = await result.total_count()
    assert total_again == 5


async def test_find_all_to_list():
    """Test that to_list() collects all results into a list."""
    dao = DaoClass()
    titles = ["Apple", "Banana", "Cherry"]
    for title in titles:
        await dao.insert(InventoryItem(title=title, count=1))

    result = dao.find_all(mapping={}, sort=["title"])
    items = await result.to_list()
    assert [item.title for item in items] == ["Apple", "Banana", "Cherry"]


async def test_nested_sort_in_find_all():
    """Test that `sort` works correctly in find_all() when the field is nested."""

    class InnerDto(BaseModel):
        x: int

    class OuterDto(BaseModel):
        id: int
        inner: InnerDto

    OuterDtoDao = new_mock_dao_class(dto_model=OuterDto, id_field="id")  # noqa: N806
    dao = OuterDtoDao()

    dtos = [
        OuterDto(id=1, inner=InnerDto(x=3)),
        OuterDto(id=2, inner=InnerDto(x=1)),
        OuterDto(id=3, inner=InnerDto(x=7)),
    ]

    for dto in dtos:
        await dao.insert(dto)

    find_all = dao.find_all

    # First retrieve results and make sure they're returned in the order defined above
    assert [x.id async for x in find_all(mapping={})] == [1, 2, 3]

    # Now apply ascending and descending sorting on the nested field
    assert [x.id async for x in find_all(mapping={}, sort=["inner.x"])] == [2, 1, 3]
    assert [x.id async for x in find_all(mapping={}, sort=["-inner.x"])] == [3, 1, 2]


async def test_sort_field_starting_with_id_field():
    """Test that only the exact ID field is translated to the internal "_id" field.

    A field whose name merely starts with the ID field (e.g. "id_code" when
    the ID field is "id") must be left untouched.
    """

    class ItemDto(BaseModel):
        id: int
        id_code: int

    ItemDtoDao = new_mock_dao_class(dto_model=ItemDto, id_field="id")  # noqa: N806
    dao = ItemDtoDao()

    dtos = [
        ItemDto(id=1, id_code=30),
        ItemDto(id=2, id_code=10),
        ItemDto(id=3, id_code=20),
    ]
    for dto in dtos:
        await dao.insert(dto)

    # Sorting by the exact ID field maps to the internal "_id" field
    assert [x.id async for x in dao.find_all(mapping={}, sort=["id"])] == [1, 2, 3]
    assert [x.id async for x in dao.find_all(mapping={}, sort=["-id"])] == [3, 2, 1]

    # Sorting by the prefix-colliding field sorts by *that* field, not the ID field
    assert [x.id async for x in dao.find_all(mapping={}, sort=["id_code"])] == [2, 3, 1]


async def test_sort_with_missing_values_matches_mongodb():
    """Test that missing values are sorted consistently, matching MongoDB.

    Missing/None values are grouped together and sorted ahead of present values in
    ascending order (and after them in descending order), matching MongoDB. This also
    exercises comparisons between None and present values, which would raise a TypeError
    without the None-safe sort key.
    """

    class InnerDto(BaseModel):
        x: int

    class OuterDto(BaseModel):
        id: int
        inner: InnerDto | None = None

    OuterDtoDao = new_mock_dao_class(dto_model=OuterDto, id_field="id")  # noqa: N806
    dao = OuterDtoDao()

    dtos = [
        OuterDto(id=1, inner=InnerDto(x=3)),
        OuterDto(id=2, inner=None),
        OuterDto(id=3, inner=InnerDto(x=1)),
        OuterDto(id=4, inner=None),
    ]
    for dto in dtos:
        await dao.insert(dto)

    # Ascending: missing (None) values first (insertion order kept among them), then
    # present values in ascending order.
    asc = [x.id async for x in dao.find_all(mapping={}, sort=["inner.x"])]
    assert asc == [2, 4, 3, 1]

    # Descending: present values in descending order, missing values last.
    desc = [x.id async for x in dao.find_all(mapping={}, sort=["-inner.x"])]
    assert desc == [1, 3, 2, 4]


async def test_nested_id_sort_matches_mongodb():
    """The in-mem DAO matches MongoDB's (limited) behavior: it cannot sort by a nested
    sub-field of the ID field, because the ID is stored under "_id". Such a sort is a
    no-op that preserves the existing order rather than raising or secretly resolving
    the value - exactly as MongoDB would silently fail to sort by "inner.x".
    """

    class InnerDto(BaseModel):
        def __hash__(self) -> int:
            return self.x

        x: int

    class OuterDto(BaseModel):
        inner: InnerDto
        label: int

    OuterDtoDao = new_mock_dao_class(dto_model=OuterDto, id_field="inner")  # noqa: N806
    dao = OuterDtoDao()

    dtos = [
        OuterDto(inner=InnerDto(x=3), label=1),
        OuterDto(inner=InnerDto(x=1), label=2),
        OuterDto(inner=InnerDto(x=7), label=3),
    ]
    for dto in dtos:
        await dao.insert(dto)

    # Sorting by a nested sub-field of the ID field is a no-op (insertion order kept)
    assert [x.label async for x in dao.find_all(mapping={}, sort=["inner.x"])] == [
        1,
        2,
        3,
    ]

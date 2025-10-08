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

import pytest
from pydantic import BaseModel

from hexkit.protocols.dao import NoHitsFoundError, ResourceNotFoundError
from hexkit.providers.testing import MockDAOEmptyError, get_dao

pytestmark = pytest.mark.asyncio()


class InventoryItem(BaseModel):
    """A model that can be used for testing"""

    title: str
    count: int


DaoClass = get_dao(dto_model=InventoryItem, id_field="title")


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

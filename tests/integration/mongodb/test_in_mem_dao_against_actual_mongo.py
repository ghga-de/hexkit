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

"""Tests meant to verify the reliability of the InMemDao"""

from collections.abc import AsyncGenerator, Mapping
from datetime import datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from pydantic import BaseModel, Field

from hexkit.protocols.dao import Dao
from hexkit.providers.mongodb.provider import MongoDbDaoFactory
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)
from hexkit.providers.testing import new_mock_dao_class
from hexkit.utils import now_utc_ms_prec

pytestmark = pytest.mark.asyncio()

DATE1 = now_utc_ms_prec() + timedelta(weeks=1)
DATE2 = now_utc_ms_prec() + timedelta(weeks=2)
DATE3 = now_utc_ms_prec() + timedelta(weeks=3)
DATE4 = now_utc_ms_prec() + timedelta(weeks=4)


class OtherData(BaseModel):
    """A model with the amount of the object sold last week and next restock date"""

    sold_last_week: int
    next_restock: datetime


class InventoryItem(BaseModel):
    """A model that can be used for testing"""

    title: str
    count: int
    other_data: OtherData


class InventoryCategory(BaseModel):
    """A model that features nesting and lists"""

    title: str
    items: list[InventoryItem] = Field(default=[])
    top_item: InventoryItem = Field(default=...)


# Real DAOs
InventoryItemDao = Dao[InventoryItem]
InventoryCategoryDao = Dao[InventoryCategory]

# Mock DAOs
MockItemDaoClass = new_mock_dao_class(dto_model=InventoryItem, id_field="title")
MockCategoryDaoClass = new_mock_dao_class(dto_model=InventoryCategory, id_field="title")

# Define some test objects
APPLES = InventoryItem(
    title="apples",
    count=101,
    other_data=OtherData(sold_last_week=25, next_restock=DATE1),
)
BROCCOLI = InventoryItem(
    title="broccoli",
    count=7,
    other_data=OtherData(sold_last_week=18, next_restock=DATE1),
)
CELERY = InventoryItem(
    title="celery",
    count=40,
    other_data=OtherData(sold_last_week=12, next_restock=DATE2),
)
PRODUCE = InventoryCategory(
    title="produce", items=[APPLES, BROCCOLI, CELERY], top_item=APPLES
)

BRAKE_PADS = InventoryItem(
    title="brake pads",
    count=254,
    other_data=OtherData(sold_last_week=35, next_restock=DATE3),
)
CHAIN = InventoryItem(
    title="chain",
    count=500,
    other_data=OtherData(sold_last_week=55, next_restock=DATE4),
)
BIKE_PARTS = InventoryCategory(
    title="bike parts", items=[BRAKE_PADS, CHAIN], top_item=BRAKE_PADS
)
CATEGORIES_COLL_NAME = "categories"
ITEMS_COLL_NAME = "items"


@pytest_asyncio.fixture(name="mock_category_dao")
async def populated_category_dao():
    """Provides a populated InMemDao for InventoryCategory objects"""
    dao = MockCategoryDaoClass()
    await dao.insert(PRODUCE)
    await dao.insert(BIKE_PARTS)
    return dao


@pytest_asyncio.fixture(name="mock_item_dao")
async def populated_item_dao():
    """Provides a populated InMemDao for InventoryItem objects"""
    dao = MockItemDaoClass()
    await dao.insert(APPLES)
    await dao.insert(BROCCOLI)
    await dao.insert(CELERY)
    await dao.insert(BRAKE_PADS)
    await dao.insert(CHAIN)
    return dao


@pytest_asyncio.fixture(name="real_category_dao")
async def populated_real_category_dao(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[InventoryCategoryDao]:
    """Provides a ready-to-use synchronous db with test data inserted"""
    async with MongoDbDaoFactory.construct(config=mongodb.config) as dao_factory:
        dao = await dao_factory.get_dao(
            name=CATEGORIES_COLL_NAME,
            dto_model=InventoryCategory,
            id_field="title",
        )
        for x in [PRODUCE, BIKE_PARTS]:
            await dao.insert(x)
        yield dao


@pytest_asyncio.fixture(name="real_item_dao")
async def populated_real_item_dao(
    mongodb: MongoDbFixture,
) -> AsyncGenerator[InventoryItemDao]:
    """Provides a ready-to-use InventoryItem DAO with test data inserted"""
    async with MongoDbDaoFactory.construct(config=mongodb.config) as dao_factory:
        dao = await dao_factory.get_dao(
            name=ITEMS_COLL_NAME,
            dto_model=InventoryItem,
            id_field="title",
        )
        for x in [APPLES, BROCCOLI, CELERY, BRAKE_PADS, CHAIN]:
            await dao.insert(x)
        yield dao


@pytest.mark.parametrize(
    "mapping, results",
    [
        (
            {"top_item.other_data.sold_last_week": {"$exists": True}},
            ["bike parts", "produce"],
        ),
        (
            {"top_item": {"$exists": True}},
            ["bike parts", "produce"],
        ),
    ],
    ids=["ExistsOnNestedField", "ExistsTopLevelField"],
)
async def test_with_category_dao(
    real_category_dao, mock_category_dao, mapping: Mapping[str, Any], results: list[str]
):
    """Test that dot notation is interpreted by the InMemDao"""
    mongodb_results = [
        x.title async for x in real_category_dao.find_all(mapping=mapping)
    ]
    inmem_results = [x.title async for x in mock_category_dao.find_all(mapping=mapping)]
    assert sorted(mongodb_results) == results, "MongoDB results not as expected"
    assert sorted(inmem_results) == results, "Mock DAO results not as expected"


@pytest.mark.parametrize(
    "mapping, results",
    [
        ({"count": {"$lt": 200, "$ne": 7}}, ["apples", "celery"]),
        (
            {
                "$or": [{"count": {"$gt": 200}}, {"count": {"$eq": 7}}],
                "title": {"$in": ["chain", "broccoli", "apples"]},
            },
            ["broccoli", "chain"],
        ),
    ],
    ids=["OneFieldMultipleOps", "MultipleFieldsMultipleOps"],
)
async def test_with_item_dao(
    real_item_dao, mock_item_dao, mapping: Mapping[str, Any], results: list[str]
):
    """Test that dot notation is interpreted by the InMemDao"""
    mongodb_results = [x.title async for x in real_item_dao.find_all(mapping=mapping)]
    inmem_results = [x.title async for x in mock_item_dao.find_all(mapping=mapping)]
    assert sorted(mongodb_results) == results, "MongoDB results not as expected"
    assert sorted(inmem_results) == results, "Mock DAO results not as expected"


@pytest.mark.parametrize(
    "mapping, results",
    [
        ({"top_item.other_data": APPLES.other_data.model_dump()}, ["produce"]),
        ({"title.notreal": None}, ["bike parts", "produce"]),
    ],
    ids=["SimpleNest", "MatchNonExistentField"],
)
async def test_also_on_non_mql(
    real_category_dao, mock_category_dao, mapping: Mapping[str, Any], results: list[str]
):
    """Tests for direct equality of a nested field with MQL enabled and disabled"""
    mongodb_results = [
        x.title async for x in real_category_dao.find_all(mapping=mapping)
    ]
    inmem_results = [x.title async for x in mock_category_dao.find_all(mapping=mapping)]
    assert sorted(mongodb_results) == results, "MongoDB results not as expected"
    assert sorted(inmem_results) == results, "Mock DAO results not as expected"

    # Make sure that DAOs that don't handle MQL still support dot notation
    CategoryDaoClassNoMQL = new_mock_dao_class(  # noqa: N806
        dto_model=InventoryCategory, id_field="title", handle_mql=False
    )
    dao = CategoryDaoClassNoMQL()
    await dao.insert(PRODUCE)
    await dao.insert(BIKE_PARTS)
    nomql_results = [x.title async for x in dao.find_all(mapping=mapping)]
    assert nomql_results == inmem_results, "Non-MQL Mock DAO results not as expected"

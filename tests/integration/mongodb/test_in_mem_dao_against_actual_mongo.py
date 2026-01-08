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

from typing import Any

import pytest
import pytest_asyncio
from pydantic import BaseModel, Field

from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)
from hexkit.providers.testing import new_mock_dao_class

pytestmark = pytest.mark.asyncio()


class InventoryItem(BaseModel):
    """A model that can be used for testing"""

    title: str
    count: int


class InventoryCategory(BaseModel):
    """A model that features nesting and lists"""

    title: str
    items: list[InventoryItem] = Field(default=[])
    top_item: InventoryItem = Field(default=...)


ItemDaoClass = new_mock_dao_class(dto_model=InventoryItem, id_field="title")
CategoryDaoClass = new_mock_dao_class(dto_model=InventoryCategory, id_field="title")

APPLES = InventoryItem(title="apples", count=101)
BROCCOLI = InventoryItem(title="broccoli", count=7)
CELERY = InventoryItem(title="celery", count=40)
PRODUCE = InventoryCategory(
    title="produce", items=[APPLES, BROCCOLI, CELERY], top_item=APPLES
)

BRAKE_PADS = InventoryItem(title="brake pads", count=254)
CHAIN = InventoryItem(title="chain", count=500)
BIKE_PARTS = InventoryCategory(
    title="bike parts", items=[BRAKE_PADS, CHAIN], top_item=BRAKE_PADS
)
CATEGORIES_COLL_NAME = "categories"
ITEMS_COLL_NAME = "items"


@pytest_asyncio.fixture(name="category_dao")
async def populated_category_dao():
    """Provides a populated InMemDao for InventoryCategory objects"""
    dao = CategoryDaoClass()
    await dao.insert(PRODUCE)
    await dao.insert(BIKE_PARTS)
    return dao


@pytest_asyncio.fixture(name="item_dao")
async def populated_item_dao():
    """Provides a populated InMemDao for InventoryItem objects"""
    dao = ItemDaoClass()
    await dao.insert(APPLES)
    await dao.insert(BROCCOLI)
    await dao.insert(CELERY)
    await dao.insert(BRAKE_PADS)
    await dao.insert(CHAIN)
    return dao


@pytest.fixture()
def populated_db(mongodb: MongoDbFixture):
    """Provides a ready-to-use synchronous db with test data inserted"""
    client = mongodb.client
    db = client.get_database(mongodb.config.db_name)
    coll1 = db[CATEGORIES_COLL_NAME]
    coll1.insert_many([x.model_dump() for x in [PRODUCE, BIKE_PARTS]])
    coll2 = db[ITEMS_COLL_NAME]
    coll2.insert_many(
        [x.model_dump() for x in [APPLES, BROCCOLI, CELERY, BRAKE_PADS, CHAIN]]
    )
    return db


def compare_results(
    mongodb_results: list[dict[str, Any]],
    in_mem_dao_results: list[BaseModel],
    sort_key: str,
):
    """Compares a list of dicts from an actual MongoDB instance against a list of
    pydantic models from a DAO and raises an assertion error if they are not the same.
    """
    for mongodb_result, in_mem_dao_result in zip(
        sorted(mongodb_results, key=lambda doc: doc[sort_key]),
        sorted(in_mem_dao_results, key=lambda model: getattr(model, sort_key)),
        strict=True,
    ):
        del mongodb_result["_id"]
        assert mongodb_result == in_mem_dao_result.model_dump(), (
            "InMemDao result differs from MongoDB result"
        )


async def test_targeting_nested_fields(populated_db, category_dao):
    """Test that dot notation is interpreted by the InMemDao"""
    mapping = {"top_item.title": BRAKE_PADS.title}
    mongodb_results = populated_db[CATEGORIES_COLL_NAME].find(mapping).to_list()
    inmem_results = [x async for x in category_dao.find_all(mapping=mapping)]
    compare_results(mongodb_results, inmem_results, sort_key="title")


async def test_one_field_multiple_ops(populated_db, item_dao):
    """Test multiple operators for same field"""
    mapping = {"count": {"$lt": 200, "$ne": 7}}
    mongodb_results = populated_db[ITEMS_COLL_NAME].find(mapping).to_list()
    inmem_results = [x async for x in item_dao.find_all(mapping=mapping)]
    compare_results(mongodb_results, inmem_results, sort_key="title")


async def test_multiple_fields_multiple_ops(populated_db, item_dao):
    """Test using multiple fields with multiple operators each"""
    mapping = {
        "$or": [{"count": {"$gt": 200}}, {"count": {"$eq": 7}}],
        "title": {"$in": ["chain", "broccoli", "apples"]},
    }
    # should get us chain and broccoli
    mongodb_results = populated_db[ITEMS_COLL_NAME].find(mapping).to_list()
    inmem_results = [x async for x in item_dao.find_all(mapping=mapping)]
    compare_results(mongodb_results, inmem_results, sort_key="title")

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

"""Test MongoDB-based providers."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytest
from pydantic import UUID4, BaseModel, ConfigDict, Field

from hexkit.protocols.dao import (
    InvalidFindMappingError,
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    UUID4Field,
)
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()

EXAMPLE_ID1 = uuid.UUID("73ab9fd9-edce-4c7a-89fa-f31f7cabbd0b", version=4)


class ExampleDtoBase(BaseModel):
    """Example DTO model with an auto-generated ID."""

    model_config = ConfigDict(frozen=True)
    field_a: str = Field(default="test")
    field_b: int = Field(default="42")
    field_c: bool = Field(default=True)
    field_d: datetime = Field(default_factory=datetime.now)


class ExampleDtoWithTypedId(ExampleDtoBase):
    """Example DTO model with a typed UUID4 ID."""

    id: UUID4 = UUID4Field()


class ExampleDtoWithUntypedId(ExampleDtoBase):
    """Example DTO model with an untyped UUID4 ID."""

    id: str = Field(default_factory=uuid.uuid4)


ExampleDto = ExampleDtoWithTypedId


async def test_dao_find_all_with_id(mongodb: MongoDbFixture, dto_model):
    """Test using the id field as part of the mapping in find_all()"""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    # insert an example resource:
    resource = ExampleDto()
    await dao.insert(resource)

    str_id = str(resource.id)

    # retrieve the resource with find_all
    resources_read = [x async for x in dao.find_all(mapping={"id": str_id})]
    assert len(resources_read) == 1
    assert resources_read[0].id == resource.id

    # make sure the previous check wasn't a false positive
    no_results = [x async for x in dao.find_all(mapping={"id": "noresults"})]
    assert len(no_results) == 0

    # make sure other fields beside ID aren't getting ignored
    no_results_multifield = [
        x async for x in dao.find_all(mapping={"id": str_id, "field_b": 134293487})
    ]
    assert len(no_results_multifield) == 0

    multifield_found = [
        x
        async for x in dao.find_all(mapping={"id": str_id, "field_b": resource.field_b})
    ]
    assert len(multifield_found) == 1
    assert multifield_found[0] == resource

    # find_one calls find_all, so double check that it works there too
    result = await dao.find_one(mapping={"id": str_id})
    assert result == resource


async def test_dao_find_all_without_collection(mongodb: MongoDbFixture):
    """Test calling find_all() when there is no collection."""
    dao = await mongodb.dao_factory.get_dao(
        name="does-not-exist-at-all",
        dto_model=ExampleDto,
        id_field="id",
    )

    found = dao.find_all(mapping={})
    assert found is not None

    # retrieve the resource with find_all
    resources_read = [x async for x in found]
    assert len(resources_read) == 0


async def test_empty_collections(mongodb: MongoDbFixture):
    """Make sure mongo reset function works"""
    db = mongodb.client[mongodb.config.db_name]
    for i in range(1, 4):
        db.create_collection(f"test{i}")

    assert len(db.list_collection_names()) == 3

    mongodb.empty_collections(collections=["test3"])
    assert set(db.list_collection_names()) == {"test1", "test2"}
    mongodb.empty_collections(exclude_collections=["test2"])
    assert db.list_collection_names() == ["test2"]
    mongodb.empty_collections()
    assert db.list_collection_names() == []


async def test_dao_insert_happy(mongodb: MongoDbFixture):
    """Tests the happy path of inserting a new resource."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id=EXAMPLE_ID1, field_a="test1", field_b=27, field_c=True)
    await dao.insert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get_by_id(resource.id)
    assert resource == resource_observed


async def test_dao_insert_duplicate_id(mongodb: MongoDbFixture):
    """Test the situation where the ID already exists in the DB."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id=EXAMPLE_ID1, field_a="test1", field_b=27, field_c=True)
    await dao.insert(resource)

    # check error is raised correctly on trying to insert duplicate
    with pytest.raises(
        ResourceAlreadyExistsError,
        match=f'The resource with the id "{EXAMPLE_ID1}" already exists.',
    ):
        await dao.insert(resource)


async def test_dao_upsert_happy(mongodb: MongoDbFixture):
    """Tests the happy path of upserting new and existing resources."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(field_a="test1", field_b=27, field_c=True)
    await dao.upsert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get_by_id(resource.id)
    assert resource == resource_observed

    # update the resource:
    resource_update = resource.model_copy(update={"field_c": False})
    await dao.upsert(resource_update)

    # check the updated resource:
    resource_update_observed = await dao.get_by_id(resource.id)
    assert resource_update == resource_update_observed


async def test_dao_get_not_found(mongodb: MongoDbFixture):
    """Tests getting a non existing resource via its ID."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    with pytest.raises(ResourceNotFoundError):
        await dao.get_by_id("my_non_existing_id_001")


async def test_dao_update_not_found(mongodb: MongoDbFixture):
    """Tests updating a non existing resource."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(field_a="test1", field_b=27, field_c=True)

    with pytest.raises(ResourceNotFoundError):
        await dao.update(resource)


async def test_dao_delete_not_found(mongodb: MongoDbFixture):
    """Tests deleting a non existing resource via its ID."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    with pytest.raises(ResourceNotFoundError):
        await dao.delete("my_non_existing_id_001")


async def test_dao_find_invalid_mapping(mongodb: MongoDbFixture):
    """Tests find_one and find_all methods with an invalid mapping."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )
    mapping = {"non_existing_field": 28}

    with pytest.raises(InvalidFindMappingError):
        _ = await dao.find_one(mapping=mapping)

    with pytest.raises(InvalidFindMappingError):
        _ = [hit async for hit in dao.find_all(mapping=mapping)]


async def test_dao_find_no_hits(mongodb: MongoDbFixture):
    """Tests find_one and find_all methods with a mapping that results in no hits."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )
    mapping = {"field_c": 28}

    with pytest.raises(NoHitsFoundError):
        _ = await dao.find_one(mapping=mapping)

    resources = [hit async for hit in dao.find_all(mapping=mapping)]
    assert len(resources) == 0


async def test_dao_find_one_with_multiple_hits(mongodb: MongoDbFixture):
    """Tests find_one with a mapping that results in multiple hits."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    # insert three identical resources (only the IDs will differ)
    for _ in range(3):
        await dao.insert(ExampleDto(field_a="test1", field_b=27, field_c=True))

    with pytest.raises(MultipleHitsFoundError):
        _ = await dao.find_one(mapping={"field_b": 27})


async def test_complex_models(mongodb: MongoDbFixture):
    """Tests whether complex pydantic models are correctly saved and retrieved."""

    # a complex model:
    class ComplexModel(BaseModel):
        model_config = ConfigDict(frozen=True)
        id: str
        some_date: datetime
        some_path: Path
        some_nested_data: ExampleDto

    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ComplexModel,
        id_field="id",
    )

    # insert an example resource:
    resource_to_create = ComplexModel(
        id="complex_data",
        some_date=datetime(2022, 10, 18, 16, 41, 34, 780735),
        some_path=Path(__file__).resolve(),
        some_nested_data=ExampleDto(
            id=EXAMPLE_ID1, field_a="a", field_b=2, field_c=True
        ),
    )
    await dao.insert(resource_to_create)

    # read the newly inserted resource:
    resource_read = await dao.get_by_id(resource_to_create.id)

    assert resource_read == resource_to_create


async def test_duplicate_uuid(mongodb: MongoDbFixture):
    """Test to illustrate how to handle duplicate UUIDs."""
    last_id: Optional[UUID4] = None

    def bad_id_factory():
        """Bad ID factory that generates duplicate IDs."""
        nonlocal last_id
        id_ = last_id
        if id_ is None:
            id_ = last_id = uuid.uuid4()
        else:
            last_id = None
        print("RETURNING", id_)
        return id_

    class SmallDto(BaseModel):
        """Small DTO class that uses a bad id generator."""

        id: UUID4 = Field(default_factory=bad_id_factory)
        field: str

    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=SmallDto,
        id_field="id",
    )

    resource = SmallDto(field="test1")
    await dao.insert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get_by_id(resource.id)
    assert resource_observed.id == resource.id
    assert resource_observed.field == "test1"

    # insert a new resource with the same ID:
    resource2 = SmallDto(field="test2")
    assert resource2.id == resource.id
    with pytest.raises(ResourceAlreadyExistsError):
        await dao.insert(resource2)

    # regenerate the resource so that it gets a new ID
    resource2 = SmallDto(**resource2.model_dump(exclude={"id"}))

    # now it should be possible to insert the new resource:
    await dao.insert(resource2)

    # check the newly inserted resource:
    resource2_observed = await dao.get_by_id(resource2.id)
    assert resource2_observed.id == resource2.id
    assert resource2_observed.id != resource.id
    assert resource2_observed.field == "test2"

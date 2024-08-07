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

import itertools
from collections.abc import AsyncGenerator
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import BaseModel, ConfigDict

from hexkit.protocols.dao import (
    InvalidFindMappingError,
    MultipleHitsFoundError,
    NoHitsFoundError,
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
)
from hexkit.providers.mongodb.testutils import (
    MongoDbFixture,
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


class ExampleCreationDto(BaseModel):
    """Example DTO creation model."""

    model_config = ConfigDict(frozen=True)

    field_a: str
    field_b: int
    field_c: bool


class ExampleDto(ExampleCreationDto):
    """Example DTO model."""

    id: str


async def test_dao_find_all_with_id(mongodb: MongoDbFixture):
    """Test using the id field as part of the mapping in find_all()"""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        dto_creation_model=ExampleCreationDto,
        id_field="id",
    )

    # insert an example resource:
    resource_to_create = ExampleCreationDto(field_a="test1", field_b=27, field_c=True)
    resource_inserted = await dao.insert(resource_to_create)

    assert isinstance(resource_inserted, ExampleDto)
    assert (
        resource_inserted.model_dump(exclude={"id"}) == resource_to_create.model_dump()
    )

    # retrieve the resource with find_all
    resources_read = [
        x async for x in dao.find_all(mapping={"id": resource_inserted.id})
    ]
    assert len(resources_read) == 1
    assert resources_read[0].id == resource_inserted.id

    # make sure the previous check wasn't a false positive
    no_results = [x async for x in dao.find_all(mapping={"id": "noresults"})]
    assert len(no_results) == 0

    # make sure other fields beside ID aren't getting ignored
    no_results_multifield = [
        x
        async for x in dao.find_all(
            mapping={"id": resource_inserted.id, "field_b": 134293487}
        )
    ]
    assert len(no_results_multifield) == 0

    multifield_found = [
        x
        async for x in dao.find_all(
            mapping={"id": resource_inserted.id, "field_b": resource_inserted.field_b}
        )
    ]
    assert len(multifield_found) == 1
    assert multifield_found[0] == resource_inserted

    # find_one calls find_all, so double check that it works there too
    result = await dao.find_one(mapping={"id": resource_inserted.id})
    assert result == resource_inserted


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


async def test_dao_happy(mongodb: MongoDbFixture):
    """Test the happy path of performing basic CRUD database interactions using
    the MongoDbDaoFactory in a surrogate ID setting.
    """
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        dto_creation_model=ExampleCreationDto,
        id_field="id",
    )

    # insert an example resource:
    resource_to_create = ExampleCreationDto(field_a="test1", field_b=27, field_c=True)
    resource_inserted = await dao.insert(resource_to_create)

    assert isinstance(resource_inserted, ExampleDto)
    assert (
        resource_inserted.model_dump(exclude={"id"}) == resource_to_create.model_dump()
    )

    # read the newly inserted resource:
    resource_read = await dao.get_by_id(resource_inserted.id)

    assert resource_read == resource_inserted

    # update the resource:
    resource_update = resource_inserted.model_copy(update={"field_c": False})
    await dao.update(resource_update)

    # read the updated resource again:
    resource_updated = await dao.get_by_id(resource_inserted.id)

    assert resource_update == resource_updated

    # insert additional resources:
    add_resources_to_create = (
        ExampleCreationDto(field_a="test2", field_b=27, field_c=True),
        ExampleCreationDto(field_a="test3", field_b=27, field_c=False),
    )
    add_resources_inserted = [
        await dao.insert(resource) for resource in add_resources_to_create
    ]

    # perform a search for multiple resources:
    obtained_hits = {
        hit async for hit in dao.find_all(mapping={"field_b": 27, "field_c": False})
    }

    expected_hits = {resource_updated, add_resources_inserted[1]}
    assert obtained_hits == expected_hits

    # find a single resource:
    obtained_hit = await dao.find_one(mapping={"field_a": "test1"})

    assert obtained_hit == resource_updated

    # delete the resource:
    await dao.delete(resource_inserted.id)

    # confirm that the resource was deleted:
    with pytest.raises(ResourceNotFoundError):
        _ = await dao.get_by_id(resource_inserted.id)


async def test_dao_insert_natural_id_happy(mongodb: MongoDbFixture):
    """Tests the happy path of inserting a new resource in a natural ID setting."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id="example_001", field_a="test1", field_b=27, field_c=True)
    await dao.insert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get_by_id(resource.id)
    assert resource == resource_observed

    # check error is raised correctly on trying to insert duplicate
    with pytest.raises(
        ResourceAlreadyExistsError,
        match='The resource with the id "example_001" already exists.',
    ):
        await dao.insert(resource)


async def test_dao_upsert_natural_id_happy(mongodb: MongoDbFixture):
    """Tests the happy path of upserting new and existing resources in a natural ID
    setting.
    """
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id="example_001", field_a="test1", field_b=27, field_c=True)
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
        _ = await dao.get_by_id("my_non_existing_id_001")


async def test_dao_update_not_found(mongodb: MongoDbFixture):
    """Tests updating a non existing resource."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(
        id="my_non_existing_id_001", field_a="test1", field_b=27, field_c=True
    )

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
        dto_creation_model=ExampleCreationDto,
        id_field="id",
    )

    # insert three identical resources (we are in a surrogate ID setting so the
    # created resources will differ in ID):
    resource_blueprint = ExampleCreationDto(field_a="test1", field_b=27, field_c=True)
    for _ in range(3):
        _ = await dao.insert(resource_blueprint)

    with pytest.raises(MultipleHitsFoundError):
        _ = await dao.find_one(mapping={"field_b": 27})


async def test_custom_id_generator(mongodb: MongoDbFixture):
    """Tests find_one with a mapping that results in multiple hits."""

    # define a custom generator:
    async def prefixed_count_id_generator(
        prefix: str, count_offset: int = 1
    ) -> AsyncGenerator[str, None]:
        """A generator that yields IDs by counting upwards und prefixing that counts
        with a predefined string.
        """
        for count in itertools.count(start=count_offset):
            yield f"{prefix}-{count}"

    count_offset = 10
    prefix = "my-test-id"
    my_test_id_generator = prefixed_count_id_generator(
        prefix=prefix, count_offset=count_offset
    )

    # use that custom generator for inserting 3 resources:
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        dto_creation_model=ExampleCreationDto,
        id_field="id",
        id_generator=my_test_id_generator,
    )

    resource_blueprint = ExampleCreationDto(field_a="test1", field_b=27, field_c=True)
    for count in range(3):
        resource_inserted = await dao.insert(resource_blueprint)
        assert resource_inserted.id == f"{prefix}-{count_offset+count}"


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
            id="nested_data", field_a="a", field_b=2, field_c=True
        ),
    )
    await dao.insert(resource_to_create)

    # read the newly inserted resource:
    resource_read = await dao.get_by_id(resource_to_create.id)

    assert resource_read == resource_to_create

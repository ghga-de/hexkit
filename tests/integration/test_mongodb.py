# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

import pytest
from pydantic import BaseModel

from hexkit.protocols.dao import (
    InvalidFindMappingError,
    MultipleHitsFoundError,
    ResourceNotFoundError,
)
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture


class ExampleCreationDto(BaseModel):
    """Example DTO creation model."""

    field_a: str
    field_b: int
    field_c: bool

    class Config:
        frozen = True


class ExampleDto(ExampleCreationDto):
    """Example DTO model."""

    id: str


@pytest.mark.asyncio
async def test_dao_happy(mongodb_fixture: MongoDbFixture):  # noqa: F811
    """Test the happy path of performing basic CRUD database interactions using
    the MongoDbDaoFactory in a surrograte ID setting."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        dto_creation_model=ExampleCreationDto,
        id_field="id",
    )

    # insert an example resource:
    resource_to_create = ExampleCreationDto(field_a="test1", field_b=27, field_c=True)
    resource_inserted = await dao.insert(resource_to_create)

    assert isinstance(resource_inserted, ExampleDto)
    assert resource_inserted.dict(exclude={"id"}) == resource_to_create.dict()

    # read the newly inserted resource:
    resource_read = await dao.get(id_=resource_inserted.id)

    assert resource_read == resource_inserted

    # update the resource:
    resource_update = resource_inserted.copy(update={"field_c": False})
    await dao.update(resource_update)

    # read the updated resource again:
    resource_updated = await dao.get(id_=resource_inserted.id)

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
    await dao.delete(id_=resource_inserted.id)

    # confirm that the resource was deleted:
    with pytest.raises(ResourceNotFoundError):
        _ = await dao.get(id_=resource_inserted.id)


@pytest.mark.asyncio
async def test_dao_insert_natural_id_happy(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests the happy path of inserting a new resource in a natural ID setting."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id="example_001", field_a="test1", field_b=27, field_c=True)
    await dao.insert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get(id_=resource.id)
    assert resource == resource_observed


@pytest.mark.asyncio
async def test_dao_upsert_natural_id_happy(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests the happy path of upserting new and existing resources in a natural ID
    setting."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(id="example_001", field_a="test1", field_b=27, field_c=True)
    await dao.upsert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get(id_=resource.id)
    assert resource == resource_observed

    # update the resource:
    resource_update = resource.copy(update={"field_c": False})
    await dao.upsert(resource_update)

    # check the updated resource:
    resource_update_observed = await dao.get(id_=resource.id)
    assert resource_update == resource_update_observed


@pytest.mark.asyncio
async def test_dao_get_not_found(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests getting a non existing resource via its ID."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    with pytest.raises(ResourceNotFoundError):
        _ = await dao.get(id_="my_non_existing_id_001")


@pytest.mark.asyncio
async def test_dao_update_not_found(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests updating a non existing resource."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto(
        id="my_non_existing_id_001", field_a="test1", field_b=27, field_c=True
    )

    with pytest.raises(ResourceNotFoundError):
        await dao.update(resource)


@pytest.mark.asyncio
async def test_dao_delete_not_found(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests deleting a non existing resource via its ID."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    with pytest.raises(ResourceNotFoundError):
        await dao.delete(id_="my_non_existing_id_001")


@pytest.mark.asyncio
async def test_dao_find_invalid_mapping(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests find_one and find_all methods with an invalid mapping."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )
    mapping = {"non_existing_field": 28}

    with pytest.raises(InvalidFindMappingError):
        _ = await dao.find_one(mapping=mapping)

    with pytest.raises(InvalidFindMappingError):
        _ = [hit async for hit in dao.find_all(mapping=mapping)]


@pytest.mark.asyncio
async def test_dao_find_no_hits(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests find_one and find_all methods with a mapping that results in no hits."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )
    mapping = {"field_c": 28}

    resource = await dao.find_one(mapping=mapping)
    assert resource is None

    resources = [hit async for hit in dao.find_all(mapping=mapping)]
    assert len(resources) == 0


@pytest.mark.asyncio
async def test_dao_find_one_with_multiple_hits(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests find_one with a mapping that results in multiple hits."""

    dao = await mongodb_fixture.dao_factory.get_dao(
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


@pytest.mark.asyncio
async def test_custom_id_generator(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
):
    """Tests find_one with a mapping that results in multiple hits."""

    # define a custom generator:
    async def prefixed_count_id_generator(
        prefix: str, count_offset: int = 1
    ) -> AsyncGenerator[str, None]:
        """A generator that yields IDs by counting upwards und prefixing that counts
        with a predefined string."""

        for count in itertools.count(start=count_offset):
            yield f"{prefix}-{count}"

    count_offset = 10
    prefix = "my-test-id"
    my_test_id_generator = prefixed_count_id_generator(
        prefix=prefix, count_offset=count_offset
    )

    # use that custom generator for inserting 3 resources:
    dao = await mongodb_fixture.dao_factory.get_dao(
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

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
#

"""Test MongoDB-based providers."""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import UUID4, BaseModel, ConfigDict, Field, field_serializer

from hexkit.protocols.dao import (
    Dao,
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
from hexkit.utils import now_utc_ms_prec

pytestmark = pytest.mark.asyncio()

EXAMPLE_ID1 = uuid.UUID("73ab9fd9-edce-4c7a-89fa-f31f7cabbd0b", version=4)


class ExampleDto(BaseModel):
    """Example DTO model with an auto-generated UUID4 ID field."""

    model_config = ConfigDict(frozen=True)

    id: UUID4 = UUID4Field(description="The ID of the resource.")

    field_a: str = Field(default="test")
    field_b: int = Field(default=42)
    field_c: bool = Field(default=True)
    field_d: datetime = Field(default_factory=now_utc_ms_prec)
    field_e: Path = Field(default_factory=Path.cwd)

    @field_serializer("field_e")
    def serialize_field_e(self, value: Path) -> str:
        """Serialize field_e to a string."""
        return str(value)


class CustomIdGenerator:
    """A custom string based ID generator."""

    def __init__(self) -> None:
        self.last_id = 0

    def str_id_factory(self) -> str:
        """Factory that can be used to generate new str based IDs."""
        self.last_id += 1
        return f"id-{self.last_id}"

    def int_id_factory(self) -> int:
        """Factory that can be used to generate new int based IDs."""
        self.last_id += 1
        return self.last_id

    def composite_id_factory(self) -> tuple[str, int]:
        """Factory that can be used to generate new composite IDs."""
        return ("composite", self.int_id_factory())


class ExampleDtoWithStrID(ExampleDto):
    """Example DTO model with a custom string based ID."""

    custom_id: str = Field(
        default_factory=CustomIdGenerator().str_id_factory,
        description="Custom string based ID of the resource.",
    )


class ExampleDtoWithIntID(ExampleDto):
    """Example DTO model with a custom int based ID."""

    custom_id: int = Field(
        default_factory=CustomIdGenerator().int_id_factory,
        description="Custom int based ID of the resource.",
    )


class ComplexDto(BaseModel):
    """A more complex, nested DTO."""

    id: UUID4 = UUID4Field()
    sub: ExampleDto = Field(default_factory=ExampleDto)
    sub_tuple: tuple[str, ExampleDto] = Field(
        default_factory=lambda: ("test-tuple", ExampleDto())
    )

    sub_list: list[str | ExampleDto] = Field(
        default_factory=lambda: cast(
            list[str | ExampleDto], ["test-list", ExampleDto()]
        )
    )
    sub_dict: dict[str, ExampleDto] = Field(
        default_factory=lambda: {"test-dict": ExampleDto()}
    )


async def test_dao_find_all_with_id(mongodb: MongoDbFixture):
    """Test using the id field as part of the mapping in find_all()"""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    # insert an example resource:
    resource = ExampleDto()
    await dao.insert(resource)

    # retrieve the resource with find_all
    resources_read = [x async for x in dao.find_all(mapping={"id": resource.id})]
    assert len(resources_read) == 1
    assert resources_read[0].id == resource.id

    # make sure the previous check wasn't a false positive
    no_results = [x async for x in dao.find_all(mapping={"id": "noresults"})]
    assert len(no_results) == 0

    # make sure other fields beside ID aren't getting ignored
    no_results_multifield = [
        x async for x in dao.find_all(mapping={"id": resource.id, "field_b": 134293487})
    ]
    assert len(no_results_multifield) == 0

    multifield_found = [
        x
        async for x in dao.find_all(
            mapping={"id": resource.id, "field_b": resource.field_b}
        )
    ]
    assert len(multifield_found) == 1
    assert multifield_found[0] == resource

    # find_one calls find_all, so double check that it works there too
    result = await dao.find_one(mapping={"id": resource.id})
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

    resource = ExampleDto()
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

    resource = ExampleDto()
    await dao.insert(resource)

    # check error is raised correctly on trying to insert duplicate
    with pytest.raises(
        ResourceAlreadyExistsError,
        match=f'The resource with the id "{resource.id}" already exists.',
    ):
        await dao.insert(resource)


async def test_dao_upsert_happy(mongodb: MongoDbFixture):
    """Tests the happy path of upserting new and existing resources."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto()
    await dao.upsert(resource)

    # check the newly inserted resource:
    resource_observed = await dao.get_by_id(resource.id)
    assert resource == resource_observed

    # update the resource:
    resource_update = resource.model_copy(update={"field_c": False})
    assert resource_update != resource
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

    resource = ExampleDto()

    with pytest.raises(ResourceNotFoundError):
        await dao.update(resource)


async def test_dao_delete_happy(mongodb: MongoDbFixture):
    """Tests deleting an existing resource via its ID."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resource = ExampleDto()
    await dao.insert(resource)
    assert await dao.get_by_id(resource.id) == resource
    await dao.delete(resource.id)
    with pytest.raises(ResourceNotFoundError):
        await dao.get_by_id(resource.id)


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
        await dao.find_one(mapping=mapping)

    with pytest.raises(InvalidFindMappingError):
        [hit async for hit in dao.find_all(mapping=mapping)]


async def test_dao_find_no_hits(mongodb: MongoDbFixture):
    """Tests find_one and find_all methods with a mapping that results in no hits."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )
    mapping = {"field_c": 28}

    with pytest.raises(NoHitsFoundError):
        await dao.find_one(mapping=mapping)

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
        await dao.insert(ExampleDto())

    with pytest.raises(MultipleHitsFoundError):
        await dao.find_one(mapping={"field_b": 42})


async def test_complex_models(mongodb: MongoDbFixture):
    """Tests whether complex pydantic models are correctly saved and retrieved."""
    dao = await mongodb.dao_factory.get_dao(
        name="complex",
        dto_model=ComplexDto,
        id_field="id",
    )

    resources = []
    for i in range(3):
        nested = ExampleDto()
        resource = ComplexDto(
            sub=nested,
            sub_tuple=("test-tuple", nested),
            sub_list=["test-list", nested],
            sub_dict={"test-dict": nested},
        )
        await dao.insert(resource)
        nested_updated = ExampleDto(field_a=f"test-{i}", field_e=Path(f"/path-{i}"))
        assert nested_updated != nested
        resource_updated = ComplexDto(
            id=resource.id,
            sub=nested_updated,
            sub_tuple=("test-tuple", nested_updated),
            sub_list=["test-list", nested_updated],
            sub_dict={"test-dict": nested_updated},
        )
        await dao.update(resource_updated)
        resources.append(resource_updated)

    for i in range(3):
        resource = resources[i]

        # fetch one newly inserted resource by its ID:
        resource_read = await dao.get_by_id(resource.id)
        assert resource_read == resource

        # fetch the resource by filtering via complex mappings:
        nested = resource.sub
        mapping_sub = {
            "id": nested.id,
            "field_a": nested.field_a,
            "field_b": nested.field_b,
            "field_c": nested.field_c,
            "field_d": nested.field_d,
            "field_e": str(nested.field_e),
        }
        mappings: list[dict[str, Any]] = [
            {"id": resource.id},
            {"sub": mapping_sub},
            {"sub_tuple": ("test-tuple", mapping_sub)},
            {"sub_list": ["test-list", mapping_sub]},
            {
                "id": resource.id,
                "sub": mapping_sub,
                "sub_tuple": ("test-tuple", mapping_sub),
                "sub_list": ["test-list", mapping_sub],
            },
        ]

        for mapping in mappings:
            obtained_hit = await dao.find_one(mapping=mapping)
            assert obtained_hit == resource
            obtained_hits = [hit async for hit in dao.find_all(mapping=mapping)]
            assert obtained_hits == [resource]

    for i in range(3):
        await dao.delete(resources[i].id)
        obtained_hits = [hit async for hit in dao.find_all(mapping={})]
        assert len(obtained_hits) == 2 - i


async def test_duplicate_uuid(mongodb: MongoDbFixture):
    """Test to illustrate how to handle duplicate UUIDs."""
    last_id: UUID4 | None = None

    def bad_id_factory():
        """Bad ID factory that generates duplicate IDs."""
        nonlocal last_id
        id_ = last_id
        if id_ is None:
            id_ = last_id = uuid.uuid4()
        else:
            last_id = None
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


@pytest.mark.parametrize(
    "dto_model", [ExampleDto, ExampleDtoWithIntID, ExampleDtoWithStrID]
)
async def test_dao_crud_happy(dto_model: type, mongodb: MongoDbFixture):
    """Test the happy path of a typical CRUD database interaction in sequence.

    This tests UUID4 based as well as custom int and string based ID generators.
    """
    assert issubclass(dto_model, ExampleDto)
    id_field = "id" if dto_model is ExampleDto else "custom_id"
    dao: Dao[ExampleDto] = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=dto_model,
        id_field=id_field,
    )

    # insert an example resource:
    resource = dto_model()
    resource_id = getattr(resource, id_field)
    assert hasattr(resource, "custom_id") == (dto_model is not ExampleDto)
    await dao.insert(resource)

    # read the newly inserted resource:
    resource_read = await dao.get_by_id(resource_id)

    assert resource_read == resource

    # update the resource:
    resource_updated = resource.model_copy(update={"field_c": False})
    await dao.update(resource_updated)

    # read the updated resource again:
    resource_updated_read = await dao.get_by_id(resource_id)

    assert resource_updated_read == resource_updated

    # insert additional resources:
    resource2 = dto_model(field_a="test2", field_b=27)
    await dao.insert(resource2)
    resource3 = dto_model(field_a="test3", field_c=False)
    await dao.upsert(resource3)  # upsert should work here as well

    # perform a search for multiple resources:
    obtained_hits = {
        hit async for hit in dao.find_all(mapping={"field_b": 42, "field_c": False})
    }

    assert obtained_hits == {resource_updated, resource3}

    # perform a search using values with non-standard data types
    mapping = {id_field: resource_id, "field_d": resource.field_d}
    obtained_hit = await dao.find_one(mapping=mapping)
    assert obtained_hit == resource_updated
    obtained_hits = {hit async for hit in dao.find_all(mapping=mapping)}
    assert obtained_hits == {resource_updated}

    # make sure that 3 resources with different IDs were inserted:
    obtained_ids = {getattr(hit, id_field) async for hit in dao.find_all(mapping={})}
    assert len(obtained_ids) == 3
    assert resource_id in obtained_ids
    for obtained_id in obtained_ids:
        if dto_model is ExampleDtoWithIntID:
            assert isinstance(obtained_id, int)
        elif dto_model is ExampleDtoWithStrID:
            assert isinstance(obtained_id, str) and obtained_id.startswith("id-")
        else:
            assert isinstance(obtained_id, uuid.UUID)

    # find a single resource:
    obtained_hit = await dao.find_one(mapping={"field_a": "test3"})

    assert obtained_hit == resource3

    # delete the resource:
    await dao.delete(getattr(resource3, id_field))

    # confirm that the resource was deleted:
    with pytest.raises(NoHitsFoundError):
        obtained_hit = await dao.find_one(mapping={"field_a": "test3"})

    # make sure that only 2 resources are left:
    obtained_hits = {hit async for hit in dao.find_all(mapping={})}
    assert len(obtained_hits) == 2

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
#

"""Test the MongoDB-based DAO factory provider."""

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
    UniqueConstraintViolationError,
    UUID4Field,
)
from hexkit.providers.mongodb import MongoDbIndex
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


async def test_indexing_simple(mongodb: MongoDbFixture):
    """Verify that the indexing features work for MongoDB"""
    dao: Dao[ExampleDto] = await mongodb.dao_factory.get_dao(
        name="data",
        dto_model=ExampleDto,
        id_field="id",
        indexes=[
            MongoDbIndex(
                fields={"field_a": 1, "field_b": 1}, properties={"unique": True}
            )
        ],
    )

    dto = ExampleDto()
    dto2 = ExampleDto()

    await dao.insert(dto)

    with pytest.raises(ResourceAlreadyExistsError):
        await dao.insert(dto)

    with pytest.raises(UniqueConstraintViolationError):
        await dao.insert(dto2)

    # The first value in the unique index is different, but the second is the same
    dto3 = ExampleDto(field_a="banana")
    await dao.insert(dto3)

    # The second value in the unique index is different, but the first is the same
    dto4 = ExampleDto(field_b=700)
    await dao.insert(dto4)

    # Make sure both update and upsert perform the correct error handling too
    dto5 = dto3.model_copy(update={"field_a": dto.field_a})
    with pytest.raises(UniqueConstraintViolationError):
        await dao.update(dto5)

    with pytest.raises(UniqueConstraintViolationError):
        await dao.upsert(dto5)


async def test_apply_index_multiple_times(mongodb: MongoDbFixture):
    """Test that nothing breaks when an index is created subsequent times.

    This makes sure that there won't be problems when a service restarts after
    applying an index for the first time.
    """
    collection_name = "data"
    dao: Dao[ExampleDto] = await mongodb.dao_factory.get_dao(
        name=collection_name,
        dto_model=ExampleDto,
        id_field="id",
        indexes=[
            MongoDbIndex(
                fields={"field_a": 1, "field_b": 1}, properties={"unique": True}
            )
        ],
    )

    dto = ExampleDto()

    await dao.insert(dto)

    _ = await mongodb.dao_factory.get_dao(
        name=collection_name,
        dto_model=ExampleDto,
        id_field="id",
        indexes=[
            MongoDbIndex(
                fields={"field_a": 1, "field_b": 1}, properties={"unique": True}
            )
        ],
    )

    config = mongodb.config
    collection = mongodb.client[config.db_name][collection_name]

    indexes = collection.index_information()
    assert len(indexes) == 2  # default index and the one added above
    _ = indexes.pop("_id_")  # get rid of default index
    index = next(iter(indexes.values()))
    assert "unique" in index
    assert index["unique"] is True
    assert index["key"] == [("field_a", 1), ("field_b", 1)]


async def test_indexing_complex(mongodb: MongoDbFixture):
    """Check indexing on nested fields and such"""
    collection_name = "data"
    dao: Dao[ComplexDto] = await mongodb.dao_factory.get_dao(
        name=collection_name,
        dto_model=ComplexDto,
        id_field="id",
        indexes=[
            MongoDbIndex(
                fields={"sub.field_a": 1, "sub.field_b": 1},
                properties={"unique": True},
            )
        ],
    )

    dto = ComplexDto()
    dto2 = ComplexDto()

    await dao.insert(dto)

    with pytest.raises(ResourceAlreadyExistsError):
        await dao.insert(dto)

    with pytest.raises(UniqueConstraintViolationError):
        await dao.insert(dto2)

    config = mongodb.config
    collection = mongodb.client[config.db_name][collection_name]
    indexes = collection.index_information()
    assert len(indexes) == 2  # default index and the one added above
    _ = indexes.pop("_id_")  # get rid of default index
    index = next(iter(indexes.values()))
    assert "unique" in index
    assert index["unique"] is True
    assert index["key"] == [("sub.field_a", 1), ("sub.field_b", 1)]


async def test_compound_unique_index_with_id_field(mongodb: MongoDbFixture):
    """Verify that when a compound unique index is placed on fields which include the
    _id field (`id` on the ExampleDto), that:
    A) The field validation in DaoFactoryBase doesn't trip up
    B) The index field name actually submitted to pymongo is "_id", not the model's
    field name.

    And when that index is violated because of the _id field, a
    ResourceAlreadyExistsError is raised and not a UniqueConstraintViolationError.
    """
    dao: Dao[ExampleDto] = await mongodb.dao_factory.get_dao(
        name="data",
        dto_model=ExampleDto,
        id_field="id",
        indexes=[
            MongoDbIndex(fields={"id": 1, "field_b": 1}, properties={"unique": True})
        ],
    )

    dto = ExampleDto()
    dto2 = ExampleDto(id=dto.id)

    await dao.insert(dto)

    with pytest.raises(ResourceAlreadyExistsError):
        await dao.insert(dto2)


async def test_index_with_string_field(mongodb: MongoDbFixture):
    """Verify that an index can be created by passing just a field name string
    instead of a dict.
    """
    collection_name = "data"
    dao: Dao[ExampleDto] = await mongodb.dao_factory.get_dao(
        name=collection_name,
        dto_model=ExampleDto,
        id_field="id",
        indexes=[MongoDbIndex(fields="field_a")],
    )

    dto = ExampleDto(field_a="test_value")
    await dao.insert(dto)

    # Verify the index was created
    config = mongodb.config
    collection = mongodb.client[config.db_name][collection_name]
    indexes = collection.index_information()
    assert len(indexes) == 2  # default index and the one added above
    _ = indexes.pop("_id_")  # get rid of default index
    index = next(iter(indexes.values()))
    # When passing a string, it should create an ascending index
    assert index["key"] == [("field_a", 1)]


async def test_dao_find_all_sort(mongodb: MongoDbFixture):
    """Test that find_all() respects the sort parameter."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resources = [ExampleDto(field_b=i) for i in range(5)]
    for resource in resources:
        await dao.insert(resource)

    # sort ascending by field_b — results should be ordered 0, 1, 2, 3, 4
    asc_results = [
        hit.field_b async for hit in dao.find_all(mapping={}, sort=["field_b"])
    ]
    assert asc_results == [0, 1, 2, 3, 4]

    # sort descending by field_b — results should be ordered 4, 3, 2, 1, 0
    desc_results = [
        hit.field_b async for hit in dao.find_all(mapping={}, sort=["-field_b"])
    ]
    assert desc_results == [4, 3, 2, 1, 0]


async def test_dao_find_all_pagination(mongodb: MongoDbFixture):
    """Test that find_all() respects skip and limit (with sort for determinism)."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resources = [ExampleDto(field_b=i) for i in range(5)]
    for resource in resources:
        await dao.insert(resource)

    asc = ["field_b"]

    # skip=2 returns items with field_b in [2, 3, 4]
    skipped = [hit.field_b async for hit in dao.find_all(mapping={}, skip=2, sort=asc)]
    assert skipped == [2, 3, 4]

    # limit=3 returns items with field_b in [0, 1, 2]
    limited = [hit.field_b async for hit in dao.find_all(mapping={}, limit=3, sort=asc)]
    assert limited == [0, 1, 2]

    # skip=1, limit=2 returns items with field_b in [1, 2]
    paginated = [
        hit.field_b async for hit in dao.find_all(mapping={}, skip=1, limit=2, sort=asc)
    ]
    assert paginated == [1, 2]

    # skip larger than collection size returns nothing
    over_skip = [hit async for hit in dao.find_all(mapping={}, skip=10, sort=asc)]
    assert over_skip == []


async def test_dao_find_all_limit_zero(mongodb: MongoDbFixture):
    """Test that limit=0 returns an empty iterator but total_count still works."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    for _ in range(3):
        await dao.insert(ExampleDto())

    result = dao.find_all(mapping={}, limit=0)
    page = [hit async for hit in result]
    assert page == []
    assert await result.total_count() == 3


async def test_dao_find_all_pagination_empty_collection(mongodb: MongoDbFixture):
    """Test find_all() with skip and limit on an empty collection produces no errors."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    results = [hit async for hit in dao.find_all(mapping={}, skip=5, limit=10)]
    assert results == []


async def test_dao_find_all_pagination_negative_values(mongodb: MongoDbFixture):
    """Test find_all() raises ValueError when skip or limit are negative."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    with pytest.raises(ValueError):
        [hit async for hit in dao.find_all(mapping={}, skip=-1)]

    with pytest.raises(ValueError):
        [hit async for hit in dao.find_all(mapping={}, limit=-1)]


async def test_dao_find_all_sort_by_id_field(mongodb: MongoDbFixture):
    """Test that sorting correctly substitutes the id field with "_id"."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDtoWithIntID,
        id_field="custom_id",
    )

    for custom_id in [30, 10, 20]:
        await dao.insert(ExampleDtoWithIntID(custom_id=custom_id))

    asc = ["custom_id"]
    asc_results = [hit.custom_id async for hit in dao.find_all(mapping={}, sort=asc)]
    assert asc_results == [10, 20, 30]

    desc = ["-custom_id"]
    desc_results = [hit.custom_id async for hit in dao.find_all(mapping={}, sort=desc)]
    assert desc_results == [30, 20, 10]


async def test_dao_find_all_sort_compound(mongodb: MongoDbFixture):
    """Test that compound sort (multiple fields) works."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    await dao.insert(ExampleDto(field_a="cherry", field_b=1))
    await dao.insert(ExampleDto(field_a="banana", field_b=2))
    await dao.insert(ExampleDto(field_a="apple", field_b=2))
    await dao.insert(ExampleDto(field_a="date", field_b=2))

    # Primary: field_b asc; secondary: field_a asc (tiebreak within field_b=2 group)
    compound_sort = ["field_b", "field_a"]
    results = [
        hit.field_a async for hit in dao.find_all(mapping={}, sort=compound_sort)
    ]
    assert results == ["cherry", "apple", "banana", "date"]


async def test_dao_find_all_total_count_with_filter(mongodb: MongoDbFixture):
    """Make sure that total_count() reflects the filtered match count, not the total
    collection size.
    """
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    # Insert some docs with two different values for field_b
    for field_a in ["a", "b", "c"]:
        await dao.insert(ExampleDto(field_a=field_a, field_b=10))
    for field_a in ["d", "e"]:
        await dao.insert(ExampleDto(field_a=field_a, field_b=20))

    result = dao.find_all(mapping={"field_b": 10}, skip=1, limit=1, sort=["field_a"])
    page = [hit.field_a async for hit in result]
    assert page == ["b"]

    assert await result.total_count() == 3


async def test_dao_find_all_total_count_before_iteration(mongodb: MongoDbFixture):
    """Test that total_count() works correctly when called before consuming any results."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    for _ in range(3):
        await dao.insert(ExampleDto())

    result = dao.find_all(mapping={}, skip=1, limit=1)
    total = await result.total_count()
    assert total == 3

    page = [hit async for hit in result]
    assert len(page) == 1


async def test_dao_find_all_total_count_empty_filter_result(mongodb: MongoDbFixture):
    """Test that total_count() returns 0 when no documents match the mapping."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    await dao.insert(ExampleDto())
    await dao.insert(ExampleDto())

    result = dao.find_all(mapping={"field_b": 99999})
    page = [hit async for hit in result]
    assert page == []

    total = await result.total_count()
    assert total == 0


async def test_dao_find_all_total_count(mongodb: MongoDbFixture):
    """Test that total_count() returns the full matching count regardless of skip/limit."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resources = [ExampleDto(field_b=i) for i in range(5)]
    for resource in resources:
        await dao.insert(resource)

    result = dao.find_all(mapping={}, skip=2, limit=2, sort=["field_b"])
    page = [hit.field_b async for hit in result]
    assert page == [2, 3]

    total = await result.total_count()
    assert total == 5

    # calling total_count() a second time uses the cached value
    total_again = await result.total_count()
    assert total_again == 5


async def test_dao_find_all_to_list(mongodb: MongoDbFixture):
    """Test that to_list() collects all results into a list."""
    dao = await mongodb.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        id_field="id",
    )

    resources = [ExampleDto(field_b=i) for i in range(3)]
    for resource in resources:
        await dao.insert(resource)

    result = dao.find_all(mapping={}, sort=["field_b"])
    items = await result.to_list()
    assert isinstance(items, list)
    assert items == resources
    assert await result.total_count() == 3

    # Check that calling to_list() again returns an empty list
    assert await result.to_list() == []

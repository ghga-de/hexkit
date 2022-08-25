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

import pytest
from pydantic import BaseModel

from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.mongodb.testutils import MongoDbFixture


class ExampleCreationDto(BaseModel):
    """Example DTO creation model."""

    param_a: str
    param_b: int
    param_c: bool

    class Config:
        frozen = True


class ExampleDto(ExampleCreationDto):
    """Example DTO model."""

    id: str


@pytest.mark.asyncio
async def test_dao_factory_happy_crud(mongodb_fixture: MongoDbFixture):  # noqa: F811
    """Test the happy path of performing basic CRUD database interactions using
    the MongoDbDaoFactory."""

    dao = await mongodb_fixture.dao_factory.get_dao(
        name="example",
        dto_model=ExampleDto,
        dto_creation_model=ExampleCreationDto,
        id_field="id",
    )

    # insert an example resource:
    resource_to_create = ExampleCreationDto(param_a="test1", param_b=27, param_c=True)
    resource_inserted = await dao.insert(resource_to_create)

    assert isinstance(resource_inserted, ExampleDto)
    assert resource_inserted.dict(exclude={"id"}) == resource_to_create.dict()

    # read the newly inserted resource:
    resource_read = await dao.get(id_=resource_inserted.id)

    assert resource_read == resource_inserted

    # update the resource:
    resource_update = resource_inserted.copy(update={"param_c": False})
    await dao.update(resource_update)

    # read the updated resource again:
    resource_updated = await dao.get(id_=resource_inserted.id)

    assert resource_update == resource_updated

    # insert additional resources:
    add_resources_to_create = (
        ExampleCreationDto(param_a="test2", param_b=27, param_c=True),
        ExampleCreationDto(param_a="test3", param_b=27, param_c=False),
    )
    add_resources_inserted = [
        await dao.insert(resource) for resource in add_resources_to_create
    ]

    # perform a search for multiple resources:
    obtained_hits = {
        hit async for hit in dao.find_all(mapping={"param_b": 27, "param_c": False})
    }

    expected_hits = {resource_updated, add_resources_inserted[1]}
    assert obtained_hits == expected_hits

    # find a single resource:
    obtained_hit = await dao.find_one(mapping={"param_a": "test1"})

    assert obtained_hit == resource_updated

    # delete the resource:
    await dao.delete(id_=resource_inserted.id)

    # confirm that the resource was deleted:
    with pytest.raises(ResourceNotFoundError):
        _ = await dao.get(id_=resource_inserted.id)

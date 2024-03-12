# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Test the mongokafka providers."""

import pytest
from pydantic import BaseModel, ConfigDict

from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.providers.akafka.testutils import (  # noqa: F401
    ExpectedEvent,
    KafkaFixture,
    kafka_fixture,
)
from hexkit.providers.mongodb.testutils import (  # noqa: F401
    MongoDbFixture,
    mongodb_fixture,
)
from hexkit.providers.mongokafka import MongoKafkaConfig, MongoKafkaDaoOutboxFactory
from hexkit.providers.mongokafka.provider import CHANGE_EVENT_TYPE, DELETE_EVENT_TYPE

EXAMPLE_TOPIC = "example"


class ExampleDto(BaseModel):
    """Example DTO model."""

    id: str

    field_a: str
    field_b: int
    field_c: bool

    model_config = ConfigDict(frozen=True)


@pytest.mark.asyncio
async def test_dao_outbox_happy(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test the happy path of using the MongoKafkaOutboxFactory."""
    config = MongoKafkaConfig(
        **mongodb_fixture.config.model_dump(), **kafka_fixture.config.model_dump()
    )

    async with MongoKafkaDaoOutboxFactory.construct(config=config) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)

        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.insert(example)

        # read the newly inserted resource:
        resource_read = await dao.get_by_id(example.id)
        assert resource_read == example

        # update the resource:
        example_update = example.model_copy(update={"field_c": False})
        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload=example_update.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.update(example_update)

        # read the updated resource again:
        resource_updated = await dao.get_by_id(example.id)
        assert resource_updated == example_update

        # upsert the original state of the resource:
        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.upsert(example)

        # read the upserted resource:
        resource_read = await dao.get_by_id(example.id)
        assert resource_read == example

        # insert additional resources:
        add_examples = (
            ExampleDto(id="test2", field_a="test2", field_b=27, field_c=True),
            ExampleDto(id="test3", field_a="test3", field_b=27, field_c=False),
        )
        for add_example in add_examples:
            async with kafka_fixture.expect_events(
                events=[
                    ExpectedEvent(
                        payload=add_example.model_dump(),
                        type_=CHANGE_EVENT_TYPE,
                        key=add_example.id,
                    )
                ],
                in_topic=EXAMPLE_TOPIC,
            ):
                await dao.insert(add_example)

        # perform a search for multiple resources:
        obtained_hits = {
            hit async for hit in dao.find_all(mapping={"field_b": 27, "field_c": True})
        }

        expected_hits = {example, add_examples[0]}
        assert obtained_hits == expected_hits

        # find a single resource:
        obtained_hit = await dao.find_one(mapping={"field_a": "test1"})

        assert obtained_hit == example

        # delete the resource:
        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload={},
                    type_=DELETE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.delete(id_=example.id)

        # confirm that the resource was deleted:
        with pytest.raises(ResourceNotFoundError):
            _ = await dao.get_by_id(example.id)


@pytest.mark.asyncio
async def test_delay_publishing(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test delaying publishing of events."""
    config = MongoKafkaConfig(
        **mongodb_fixture.config.model_dump(), **kafka_fixture.config.model_dump()
    )

    async with MongoKafkaDaoOutboxFactory.construct(config=config) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
            autopublish=False,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)

        async with kafka_fixture.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            await dao.insert(example)

        assert len(recorder.recorded_events) == 0

        # publish:
        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.publish_pending()


@pytest.mark.asyncio
async def test_publishing_after_failure(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test delaying publishing after the initial publishing failed."""
    config = MongoKafkaConfig(
        **mongodb_fixture.config.model_dump(), **kafka_fixture.config.model_dump()
    )

    # a dto to event function that fails the first time:
    fail = True

    def dto_to_event(dto: ExampleDto):
        nonlocal fail

        if fail:
            fail = False
            raise RuntimeError()

        return dto.model_dump()

    async with MongoKafkaDaoOutboxFactory.construct(config=config) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=dto_to_event,
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)

        with pytest.raises(RuntimeError):
            await dao.insert(example)

        # publish:
        async with kafka_fixture.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.publish_pending()


@pytest.mark.asyncio
async def test_republishing(
    mongodb_fixture: MongoDbFixture,  # noqa: F811
    kafka_fixture: KafkaFixture,  # noqa: F811
):
    """Test republishing already published events."""
    config = MongoKafkaConfig(
        **mongodb_fixture.config.model_dump(), **kafka_fixture.config.model_dump()
    )

    async with MongoKafkaDaoOutboxFactory.construct(config=config) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)
        expected_event = ExpectedEvent(
            payload=example.model_dump(),
            type_=CHANGE_EVENT_TYPE,
            key=example.id,
        )

        async with kafka_fixture.expect_events(
            events=[expected_event],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.insert(example)

        # republish:
        async with kafka_fixture.expect_events(
            events=[expected_event],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.republish()

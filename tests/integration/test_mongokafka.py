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

"""Test the DAO pub/sub functionality based on the mongokafka/kafka providers."""

from typing import Optional

import pytest
from pydantic import BaseModel, ConfigDict

from hexkit.protocols.dao import ResourceNotFoundError
from hexkit.protocols.daosub import DaoSubscriberProtocol, DtoValidationError
from hexkit.providers.akafka import KafkaOutboxSubscriber
from hexkit.providers.akafka.testutils import ExpectedEvent, KafkaFixture
from hexkit.providers.mongokafka import MongoKafkaConfig, MongoKafkaDaoPublisherFactory
from hexkit.providers.mongokafka.provider import CHANGE_EVENT_TYPE, DELETE_EVENT_TYPE

pytestmark = pytest.mark.asyncio(scope="session")

EXAMPLE_TOPIC = "example"


class ExampleDto(BaseModel):
    """Example DTO model."""

    id: str

    field_a: str
    field_b: int
    field_c: bool

    model_config = ConfigDict(frozen=True)


class DummyOutboxSubscriber(DaoSubscriberProtocol[ExampleDto]):
    """A dummy implementation of the `DaoSubscriberProtocol` for the `ExampleDto`."""

    event_topic = EXAMPLE_TOPIC
    dto_model = ExampleDto

    def __init__(self):
        """Initialize with a `received` attribute for inspecting events consumed.
        It is a list of tuples, each containing the resource ID and, in case of a
        change event, the dto.
        """
        self.received: list[tuple[str, Optional[ExampleDto]]] = []

    async def changed(self, resource_id: str, update: ExampleDto) -> None:
        """Consume change event (created or updated) for the given resource."""
        self.received.append((resource_id, update))

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of the given resource."""
        self.received.append((resource_id, None))


async def test_dao_outbox_with_non_existing_resource(
    kafka: KafkaFixture, mongo_kafka_config: MongoKafkaConfig
):
    """Test operations on non-existing resources fail with MongoKafkaOutboxFactory."""
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        example = ExampleDto(id="test1", field_a="test", field_b=1, field_c=False)

        # first try with non-existing resource
        async with kafka.expect_events(events=[], in_topic=EXAMPLE_TOPIC):
            with pytest.raises(ResourceNotFoundError):
                await dao.get_by_id(example.id)
            with pytest.raises(ResourceNotFoundError):
                await dao.update(example)
            with pytest.raises(ResourceNotFoundError):
                await dao.delete(example.id)

        # create and delete the resource and try again with that state
        await dao.insert(example)
        await dao.delete(example.id)

        async with kafka.expect_events(events=[], in_topic=EXAMPLE_TOPIC):
            with pytest.raises(ResourceNotFoundError):
                await dao.get_by_id(example.id)
            with pytest.raises(ResourceNotFoundError):
                await dao.update(example)
            with pytest.raises(ResourceNotFoundError):
                await dao.delete(example.id)


async def test_dao_outbox_happy(
    kafka: KafkaFixture, mongo_kafka_config: MongoKafkaConfig
):
    """Test the happy path of using the MongoKafkaOutboxFactory."""
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)

        async with kafka.expect_events(
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
        async with kafka.expect_events(
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
        async with kafka.expect_events(
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
            async with kafka.expect_events(
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
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload={},
                    type_=DELETE_EVENT_TYPE,
                    key=example.id,
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.delete(example.id)

        # confirm that the resource was deleted:
        with pytest.raises(ResourceNotFoundError):
            _ = await dao.get_by_id(example.id)


async def test_delay_publishing(
    kafka: KafkaFixture,
    mongo_kafka_config: MongoKafkaConfig,
):
    """Test delaying publishing of events."""
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
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

        async with kafka.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            await dao.insert(example)

        assert len(recorder.recorded_events) == 0

        # publish:
        async with kafka.expect_events(
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


async def test_publishing_after_failure(
    kafka: KafkaFixture,
    mongo_kafka_config: MongoKafkaConfig,
):
    """Test delaying publishing after the initial publishing failed."""
    # a dto to event function that fails the first time:
    fail = True

    def dto_to_event(dto: ExampleDto):
        nonlocal fail

        if fail:
            fail = False
            raise RuntimeError()

        return dto.model_dump()

    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
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
        async with kafka.expect_events(
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


async def test_republishing(kafka: KafkaFixture, mongo_kafka_config: MongoKafkaConfig):
    """Test republishing already published events."""
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
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

        async with kafka.expect_events(
            events=[expected_event],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.insert(example)

        # republish:
        async with kafka.expect_events(
            events=[expected_event],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.republish()


async def test_dao_pub_sub_happy(mongo_kafka_config: MongoKafkaConfig):
    """Test the happy path of transmitting resource changes or deletions between the
    MongoKafkaOutboxFactory and the KafkaOutboxSubscriber.
    """
    sub_translator = DummyOutboxSubscriber()

    # publish some changes and deletions:
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto(id="test1", field_a="test1", field_b=27, field_c=True)

        await dao.insert(example)
        # update the resource:
        example_update = example.model_copy(update={"field_c": False})
        await dao.update(example_update)

        # delete the resource again:
        await dao.delete(example.id)

    expected_events = [
        (example.id, example),
        (example.id, example_update),
        (example.id, None),
    ]

    # consume events:
    async with KafkaOutboxSubscriber.construct(
        config=mongo_kafka_config,
        translators=[sub_translator],
    ) as subscriber:
        for _ in expected_events:
            await subscriber.run(forever=False)

    assert sub_translator.received == expected_events


async def test_dao_pub_sub_invalid_dto(mongo_kafka_config: MongoKafkaConfig):
    """Test that using the KafkaOutboxSubscriber to consume an event with an
    unexpected payload raises a `DtoValidationError`.
    """
    sub_translator = DummyOutboxSubscriber()

    class ProducerDTO(BaseModel):
        """producer DTO that differs from the DTO expected by the TestOutboxSubscriber"""

        id: str
        field_a: str

    # publish some changes and deletions:
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka_config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ProducerDTO,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ProducerDTO(id="test1", field_a="test1")
        await dao.insert(example)

    # consume events:
    async with KafkaOutboxSubscriber.construct(
        config=mongo_kafka_config,
        translators=[sub_translator],
    ) as subscriber:
        with pytest.raises(DtoValidationError):
            await subscriber.run(forever=False)

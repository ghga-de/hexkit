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

"""Test the DAO pub/sub functionality based on the mongokafka/kafka providers."""

import uuid
from collections.abc import Generator
from functools import partial
from pathlib import Path
from typing import Any, Optional

import pytest
from pydantic import UUID4, BaseModel
from pymongo import MongoClient
from pymongo.collection import Collection

from hexkit.correlation import (
    correlation_id_var,
    get_correlation_id,
    new_correlation_id,
    set_new_correlation_id,
)
from hexkit.protocols.dao import (
    ResourceAlreadyExistsError,
    ResourceNotFoundError,
    UUID4Field,
)
from hexkit.protocols.daosub import DaoSubscriberProtocol, DtoValidationError
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import (
    ComboTranslator,
    KafkaEventSubscriber,
    KafkaOutboxSubscriber,
)
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,  # noqa: F401
    RecordedEvent,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)
from hexkit.providers.mongodb.provider import dto_to_document
from hexkit.providers.mongodb.testutils import (
    mongodb_container_fixture,  # noqa: F401
    mongodb_fixture,  # noqa: F401
)
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory
from hexkit.providers.mongokafka.provider import (
    CHANGE_EVENT_TYPE,
    DELETE_EVENT_TYPE,
    document_to_dto,
)
from hexkit.providers.mongokafka.testutils import (
    MongoKafkaFixture,
    mongo_kafka_fixture,  # noqa: F401
)

from .test_mongodb import (
    ComplexDto,
    ExampleDto,
    ExampleDtoWithIntID,
    ExampleDtoWithStrID,
)

pytestmark = pytest.mark.asyncio()

EXAMPLE_TOPIC = "example"


@pytest.fixture(autouse=True)
def correlation_id_fixture() -> Generator[UUID4, None, None]:
    """Provides a new correlation ID for each test case."""
    # Note: Using an async fixture doesn't work reliably with older Python versions,
    # because the context is not preserved even with pytest-asyncio 0.25.
    correlation_id = new_correlation_id()
    token = correlation_id_var.set(correlation_id)
    yield correlation_id
    correlation_id_var.reset(token)


def get_mongo_collection(mongo_kafka: MongoKafkaFixture, name: str) -> Collection:
    """Get the MongoDB collection with the provided name."""
    db_name = mongo_kafka.config.db_name
    collection = mongo_kafka.mongodb.client.get_database(db_name).get_collection(name)
    return collection


class DummyOutboxSubscriber(DaoSubscriberProtocol[ExampleDto]):
    """A dummy implementation of the `DaoSubscriberProtocol` for the `ExampleDto`."""

    event_topic = EXAMPLE_TOPIC
    dto_model = ExampleDto

    def __init__(self):
        """Initialize with a `received` attribute for inspecting events consumed.
        It is a list of tuples, each containing the resource ID and, in case of a
        change event, the dto.
        """
        self.received: list[tuple[str, UUID4, Optional[ExampleDto]]] = []

    async def changed(self, resource_id: str, update: ExampleDto) -> None:
        """Consume change event (created or updated) for the given resource."""
        correlation_id = get_correlation_id()
        self.received.append((resource_id, correlation_id, update))

    async def deleted(self, resource_id: str) -> None:
        """Consume event indicating the deletion of the given resource."""
        correlation_id = get_correlation_id()
        self.received.append((resource_id, correlation_id, None))


async def test_dao_outbox_with_non_existing_resource(mongo_kafka: MongoKafkaFixture):
    """Test operations on non-existing resources fail with MongoKafkaOutboxFactory."""
    kafka = mongo_kafka.kafka
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        example = ExampleDto()

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


@pytest.mark.parametrize(
    "dto_model", [ExampleDto, ExampleDtoWithIntID, ExampleDtoWithStrID]
)
async def test_dao_outbox_happy(dto_model: type, mongo_kafka: MongoKafkaFixture):
    """Test the happy path of using the MongoKafkaOutboxFactory.

    This tests UUID4 based as well as custom int and string based ID generators.
    """
    kafka = mongo_kafka.kafka
    assert issubclass(dto_model, ExampleDto)
    id_field = "id" if dto_model is ExampleDto else "custom_id"
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=dto_model,
            id_field=id_field,
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = dto_model()
        assert hasattr(example, "custom_id") == (dto_model is not ExampleDto)
        example_id = getattr(example, id_field)

        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=str(example_id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.insert(example)

            # check error on duplicate
            with pytest.raises(
                ResourceAlreadyExistsError,
                match=f'The resource with the id "{example_id}" already exists.',
            ):
                await dao.insert(example)

        # read the newly inserted resource:
        resource_read = await dao.get_by_id(example_id)
        assert resource_read == example

        # update the resource:
        example_update = example.model_copy(update={"field_c": False})
        assert example_update != example
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload=example_update.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=str(example_id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.update(example_update)

        # read the updated resource again:
        resource_updated = await dao.get_by_id(example_id)
        assert resource_updated == example_update

        # upsert the original state of the resource:
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=str(example_id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.upsert(example)

        # read the upserted resource:
        resource_read = await dao.get_by_id(example_id)
        assert resource_read == example

        # insert additional resources:
        example2 = dto_model(field_a="test2", field_b=27)
        example3 = dto_model(field_a="test3", field_c=True)
        example4 = dto_model(field_a="test4", field_c=False)
        for add_example in (example2, example3, example4):
            async with kafka.expect_events(
                events=[
                    ExpectedEvent(
                        payload=add_example.model_dump(),
                        type_=CHANGE_EVENT_TYPE,
                        key=str(getattr(add_example, id_field)),
                    )
                ],
                in_topic=EXAMPLE_TOPIC,
            ):
                await dao.insert(add_example)

        # perform a search for multiple resources:
        obtained_hits = {
            hit async for hit in dao.find_all(mapping={"field_b": 42, "field_c": True})
        }
        assert obtained_hits == {example, example3}

        # perform a search using values with non-standard data types
        # (note that in this case we need to serialize these values manually):
        mapping = {id_field: example_id, "field_d": example.field_d}
        obtained_hit = await dao.find_one(mapping=mapping)
        assert obtained_hit == example
        obtained_hits = {hit async for hit in dao.find_all(mapping=mapping)}
        assert obtained_hits == {example}

        # make sure that 4 resources with different IDs were inserted:
        obtained_ids = {
            getattr(hit, id_field) async for hit in dao.find_all(mapping={})
        }
        assert len(obtained_ids) == 4
        assert example_id in obtained_ids
        for obtained_id in obtained_ids:
            if dto_model is ExampleDtoWithIntID:
                assert isinstance(obtained_id, int)
            elif dto_model is ExampleDtoWithStrID:
                assert isinstance(obtained_id, str) and obtained_id.startswith("id-")
            else:
                assert isinstance(obtained_id, uuid.UUID)

        # find a single resource:
        obtained_hit = await dao.find_one(mapping={"field_a": "test"})

        assert obtained_hit == example

        # delete the resource:
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload={},
                    type_=DELETE_EVENT_TYPE,
                    key=str(example_id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.delete(example_id)

        # confirm that the resource was deleted:
        with pytest.raises(ResourceNotFoundError):
            await dao.get_by_id(example_id)

        # make sure that only 3 resources are left:
        obtained_hits = {hit async for hit in dao.find_all(mapping={})}
        assert len(obtained_hits) == 3


async def test_complex_models(mongo_kafka: MongoKafkaFixture):
    """Tests whether complex pydantic models are correctly saved and retrieved."""
    async with (
        MongoKafkaDaoPublisherFactory.construct(config=mongo_kafka.config) as factory,
        mongo_kafka.kafka.record_events(
            in_topic=EXAMPLE_TOPIC, capture_headers=True
        ) as recorder,
    ):
        dao = await factory.get_dao(
            name="complex",
            dto_model=ComplexDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
            autopublish=True,
        )

        # stage 1: insert
        resources_created: list[ComplexDto] = []
        for i in range(3):
            nested = ExampleDto(field_a=f"inserted-{i}")
            resource = ComplexDto(
                sub=nested,
                sub_tuple=("test-tuple", nested),
                sub_list=["test-list", nested],
                sub_dict={"test-dict": nested},
            )
            await dao.insert(resource)
            resources_created.append(resource)

        # stage 2: update
        resources: list[ComplexDto] = []
        for i in range(3):
            resource_created = resources_created[i]
            nested = ExampleDto(field_a=f"updated-{i}", field_e=Path(f"/path-{i}"))
            assert nested != resource_created.sub
            resource = ComplexDto(
                id=resource_created.id,
                sub=nested,
                sub_tuple=("test-tuple", nested),
                sub_list=["test-list", nested],
                sub_dict={"test-dict": nested},
            )
            await dao.update(resource)
            resources.append(resource)

        # stage 3: query
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

        # stage 4: delete
        for i in range(3):
            await dao.delete(resources[i].id)
            obtained_hits = [hit async for hit in dao.find_all(mapping={})]
            assert len(obtained_hits) == 2 - i

    # check that the expected events have been created
    events: list[RecordedEvent] = list(recorder.recorded_events)
    for stage in 1, 2, 4:
        # expected events for this stage
        for i in range(3):
            resource = (resources_created if stage == 1 else resources)[i]
            event = events.pop(0)
            assert event.type_ == DELETE_EVENT_TYPE if stage == 4 else CHANGE_EVENT_TYPE
            assert event.key == str(resource.id)
            if stage == 4:
                # empty payload for deletion events
                expected_payload = {}
            else:
                # construct expected payload for change events
                sub_payload = {
                    "id": str(resource.sub.id),
                    "field_a": f"{'inserted' if stage == 1 else 'updated'}-{i}",
                    "field_b": 42,
                    "field_c": True,
                    "field_d": resource.sub.field_d.isoformat(),
                    "field_e": str(resource.sub.field_e)
                    if stage == 1
                    else f"/path-{i}",
                }
                expected_payload = {
                    "id": str(resource.id),
                    "sub": sub_payload,
                    "sub_tuple": ["test-tuple", sub_payload],
                    "sub_list": ["test-list", sub_payload],
                    "sub_dict": {"test-dict": sub_payload},
                }
            assert event.payload == expected_payload, f"stage {stage} resource {i}"
    assert not events  # no further events should have been recorded


async def test_delay_publishing(mongo_kafka: MongoKafkaFixture):
    """Test delaying publishing of events."""
    kafka = mongo_kafka.kafka
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
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
        example = ExampleDto()

        async with kafka.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            await dao.insert(example)

        assert len(recorder.recorded_events) == 0

        # publish:
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=str(example.id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.publish_pending()


async def test_suppress_publishing(mongo_kafka: MongoKafkaFixture):
    """Test suppress publishing of events."""
    kafka = mongo_kafka.kafka
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump() if dto.field_c else None,
            event_topic=EXAMPLE_TOPIC,
            autopublish=True,
        )

        # insert some example resources:
        examples = [
            ExampleDto(field_a="test1"),
            ExampleDto(field_a="test2", field_c=False),
            ExampleDto(field_a="test3"),
        ]

        async with kafka.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            for example in examples:
                await dao.insert(example)

        # check that all resources were saved:
        records = [record async for record in dao.find_all(mapping={})]
        assert len(records) == 3
        assert any(record.field_c for record in records)

        # check that the second resource was not published:
        assert len(recorder.recorded_events) == 2
        assert all(event.payload["field_c"] for event in recorder.recorded_events)


async def test_publishing_after_failure(mongo_kafka: MongoKafkaFixture):
    """Test delaying publishing after the initial publishing failed."""
    kafka = mongo_kafka.kafka

    # a dto to event function that fails the first time:
    fail = True

    def dto_to_event(dto: ExampleDto):
        nonlocal fail

        if fail:
            fail = False
            raise RuntimeError()

        return dto.model_dump()

    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=dto_to_event,
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto()

        with pytest.raises(RuntimeError):
            await dao.insert(example)

        # publish:
        async with kafka.expect_events(
            events=[
                ExpectedEvent(
                    payload=example.model_dump(),
                    type_=CHANGE_EVENT_TYPE,
                    key=str(example.id),
                )
            ],
            in_topic=EXAMPLE_TOPIC,
        ):
            await dao.publish_pending()


async def test_republishing(mongo_kafka: MongoKafkaFixture):
    """Test republishing already published events."""
    kafka = mongo_kafka.kafka
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto()
        expected_event = ExpectedEvent(
            payload=example.model_dump(),
            type_=CHANGE_EVENT_TYPE,
            key=str(example.id),
        )

        # initial insert:
        async with kafka.expect_events(events=[expected_event], in_topic=EXAMPLE_TOPIC):
            await dao.insert(example)

        # republish:
        async with kafka.expect_events(events=[expected_event], in_topic=EXAMPLE_TOPIC):
            await dao.republish()


@pytest.mark.parametrize(
    "subscriber_class", [KafkaOutboxSubscriber, KafkaEventSubscriber]
)
async def test_dao_pub_sub_happy(
    mongo_kafka: MongoKafkaFixture, subscriber_class: type[EventSubscriberProtocol]
):
    """Test the happy path of transmitting resource changes or deletions between the
    MongoKafkaOutboxFactory and the KafkaOutboxSubscriber.

    Also verify that the correlation ID is set on the event headers correctly.
    """
    sub_translator = DummyOutboxSubscriber()
    initial_correlation_id = get_correlation_id()

    # publish some changes and deletions:
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ExampleDto()

        async with set_new_correlation_id() as temp_correlation_id:
            await dao.insert(example)
            # update the resource:
            example_update = example.model_copy(update={"field_c": False})
            await dao.update(example_update)

            # delete the resource again:
            await dao.delete(example.id)

        example_id = str(example.id)
        expected_events = [
            (example_id, temp_correlation_id, example),
            (example_id, temp_correlation_id, example_update),
            (example_id, temp_correlation_id, None),
        ]

        # Verify that the context var is reverted to the initial correlation ID after
        # using the temporary one to publish the events
        assert get_correlation_id() == initial_correlation_id

        # consume events:
        construct = partial(
            KafkaOutboxSubscriber.construct, translators=[sub_translator]
        )
        if subscriber_class == KafkaEventSubscriber:
            construct = partial(
                KafkaEventSubscriber.construct,
                translator=ComboTranslator(translators=[sub_translator]),
            )

        async with construct(config=mongo_kafka.config) as subscriber:
            for _ in expected_events:
                await subscriber.run(forever=False)
            assert sub_translator.received == expected_events

            # Clear out the received list
            sub_translator.received.clear()

            # Republish and verify that the initial correlation ID is maintained
            await dao.republish()

            # Consume the republished events and check them again
            await subscriber.run(forever=False)
            assert sub_translator.received == [expected_events[-1]]


async def test_dao_pub_sub_invalid_dto(mongo_kafka: MongoKafkaFixture):
    """Test that using the KafkaOutboxSubscriber to consume an event with an
    unexpected payload raises a `DtoValidationError`.
    """
    sub_translator = DummyOutboxSubscriber()

    class ProducerDto(BaseModel):
        """Producer DTO that differs from the DTO expected by TestOutboxSubscriber"""

        id: UUID4 = UUID4Field()

        field_a: int  # this is a string field in the expected DTO

    # publish some changes and deletions:
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ProducerDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        # insert an example resource:
        example = ProducerDto(field_a=42)
        await dao.insert(example)

    # consume events:
    async with KafkaOutboxSubscriber.construct(
        config=mongo_kafka.config,
        translators=[sub_translator],
    ) as subscriber:
        with pytest.raises(DtoValidationError):
            await subscriber.run(forever=False)


async def test_mongokafka_dao_correlation_id_upsert(mongo_kafka: MongoKafkaFixture):
    """Make sure the correlation ID is set on the document metadata in upsertion.

    Insert a new document and verify that the correct correlation ID is there.
    Perform an update with a new correlation ID and verify that the correlation ID is
    updated accordingly.
    """
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        example = ExampleDto()

        await dao.insert(dto=example)

        # Verify that the inserted document contains the correlation ID
        correlation_id = get_correlation_id()
        collection = get_mongo_collection(mongo_kafka, "example")
        inserted = collection.find_one({"__metadata__.correlation_id": correlation_id})
        assert inserted
        inserted_event_id = inserted["__metadata__"].pop("last_event_id", None)
        assert isinstance(inserted_event_id, uuid.UUID)

        # Check the other metadata (now that we've removed the unpredictable event ID)
        assert inserted["__metadata__"] == {
            "correlation_id": correlation_id,
            "deleted": False,
            "published": True,
        }

        # Remove the metadata field and restore the ID field name, check against original
        inserted_as_dto = document_to_dto(inserted, id_field="id", dto_model=ExampleDto)
        assert inserted_as_dto.model_dump() == example.model_dump()

        # Create a new correlation ID to simulate a subsequent request
        async with set_new_correlation_id() as temp_correlation_id:
            assert temp_correlation_id != correlation_id

            # Update, then verify old correlation ID is overwritten by the new one
            updated_example = example.model_copy(update={"field_a": "test2"}, deep=True)
            await dao.update(updated_example)

            updated = collection.find_one(
                {"__metadata__.correlation_id": temp_correlation_id}
            )
            assert updated

            # Make sure the event ID is new since mongokafka shouldn't reuse event IDs
            updated_event_id = updated["__metadata__"].pop("last_event_id", None)
            assert isinstance(updated_event_id, uuid.UUID)
            assert updated_event_id != inserted_event_id

            assert updated["__metadata__"] == {
                "correlation_id": temp_correlation_id,
                "deleted": False,
                "published": True,
            }

            # Remove the metadata field and restore the ID field name
            updated_as_dto = document_to_dto(
                updated, id_field="id", dto_model=ExampleDto
            )
            assert updated_as_dto.model_dump() == updated_example.model_dump()

            # Verify nothing exists in the collection with the old correlation ID
            assert not collection.find_one(
                {"__metadata__.correlation_id": correlation_id}
            )


async def test_mongokafka_dao_correlation_id_delete(mongo_kafka: MongoKafkaFixture):
    """Make sure the correlation ID is set on the document metadata in deletion."""
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        example = ExampleDto()

        # Insert, then verify that the inserted document contains the correlation ID
        await dao.insert(dto=example)
        collection = get_mongo_collection(mongo_kafka, "example")
        correlation_id = get_correlation_id()
        inserted = collection.find_one({"__metadata__.correlation_id": correlation_id})
        assert inserted

        metadata = inserted.pop("__metadata__")
        assert inserted == {
            "_id": example.id,
            "field_a": "test",
            "field_b": 42,
            "field_c": True,
            "field_d": example.field_d,
            "field_e": str(example.field_e),
        }
        inserted_event_id = metadata.pop("last_event_id", None)
        assert metadata == {
            "correlation_id": correlation_id,
            "deleted": False,
            "published": True,
        }

        # Create a new correlation ID to simulate a subsequent request
        async with set_new_correlation_id() as temp_correlation_id:
            assert temp_correlation_id != correlation_id

            await dao.delete(example.id)
            deleted = collection.find_one(
                {"__metadata__.correlation_id": temp_correlation_id}
            )
            assert deleted
            metadata = deleted.pop("__metadata__")
            assert deleted == {"_id": example.id}
            deleted_event_id = metadata.pop("last_event_id", None)
            assert isinstance(deleted_event_id, uuid.UUID)
            assert deleted_event_id != inserted_event_id
            assert metadata == {
                "correlation_id": temp_correlation_id,
                "deleted": True,
                "published": True,
            }

            search_for_inserted_again = collection.find_one(
                {"__metadata__.correlation_id": correlation_id}
            )
            assert not search_for_inserted_again


async def test_documents_without_metadata(mongo_kafka: MongoKafkaFixture):
    """Test that when an Outbox Dao updates or deletes documents with no metadata, the
    metadata is added and the documents published.

    The pre-patch behavior would result in an ResourceNotFoundError.
    """
    id1 = uuid.uuid4()
    id2 = uuid.uuid4()
    example = ExampleDto(id=id1)
    doc1 = dto_to_document(example, id_field="id")
    doc2 = dto_to_document(ExampleDto(id=id2), id_field="id")

    # Insert two documents without metadata
    db_name = mongo_kafka.config.db_name
    mongo_client: MongoClient = mongo_kafka.mongodb.client
    mongo_client[db_name]["example"].insert_one(doc1)
    mongo_client[db_name]["example"].insert_one(doc2)

    # Attempt to interact with the document via MongoKafka
    async with MongoKafkaDaoPublisherFactory.construct(
        config=mongo_kafka.config
    ) as factory:
        dao = await factory.get_dao(
            name="example",
            dto_model=ExampleDto,
            id_field="id",
            dto_to_event=lambda dto: dto.model_dump(),
            event_topic=EXAMPLE_TOPIC,
        )

        async with mongo_kafka.kafka.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            # Attempt to update the document
            await dao.update(example)

        assert len(recorder.recorded_events) == 1
        assert recorder.recorded_events[0].type_ == CHANGE_EVENT_TYPE

        # Verify that the document now has metadata
        correlation_id = get_correlation_id()
        document = mongo_client[db_name]["example"].find_one({"_id": doc1["_id"]})
        assert document is not None and "__metadata__" in document
        event_id = document["__metadata__"].pop("last_event_id", None)
        assert isinstance(event_id, uuid.UUID)

        # Check the other metadata (now that we've removed the unpredictable event ID)
        assert document["__metadata__"] == {
            "correlation_id": correlation_id,
            "deleted": False,
            "published": True,
        }

        async with mongo_kafka.kafka.record_events(in_topic=EXAMPLE_TOPIC) as recorder:
            await dao.delete(id2)
        assert len(recorder.recorded_events) == 1
        assert recorder.recorded_events[0].type_ == DELETE_EVENT_TYPE

        deleted_doc = mongo_client[db_name]["example"].find_one({"_id": doc2["_id"]})
        assert deleted_doc is not None and "__metadata__" in deleted_doc
        deletion_event_id = deleted_doc["__metadata__"].pop("last_event_id", None)
        assert isinstance(event_id, uuid.UUID)
        assert deletion_event_id != event_id
        assert deleted_doc["__metadata__"] == {
            "correlation_id": correlation_id,
            "deleted": True,
            "published": True,
        }

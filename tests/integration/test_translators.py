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

"""Integration tests for the TranslatorConverter behavior.

There are 4 event schemas defined, which go to three different topics.
Two of them are outbox events, the other two are non-outbox events.
There are three translators defined: one for each of the outbox events and one
that handles both kinds of non-outbox event.
"""

from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, Field

from hexkit.custom_types import Ascii
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import KafkaEventSubscriber, TranslatorConverter
from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)

pytestmark = pytest.mark.asyncio()


class UserModel(BaseModel):
    """A test outbox event"""

    name: str = Field(default="Tom", description="User's name")


class OrderModel(BaseModel):
    """A test outbox event"""

    order_id: str = Field(default="order123", description="The order ID")
    item_id: str = Field(default="item123", description="The ordered item's ID")
    complete: bool = Field(
        default=False, description="Whether the order has been completed"
    )


class SomeEvent(BaseModel):
    """A test non-outbox event"""

    my_cool_id: str = Field(default="test123", description="Some ID field")
    some_field: int = Field(default=37, description="Some field with an int")


class SomeEvent2(BaseModel):
    """A test non-outbox event"""

    my_other_id: str = Field(default="test456", description="Some other ID field")
    some_other_field: int = Field(
        default=73, description="Some other field with an int"
    )


class OutboxUserTranslator(DaoSubscriberProtocol):
    """A translator class to subscribe to UserModel events"""

    dto_model = UserModel
    event_topic = "users"

    def __init__(self):
        self.upserts: list[tuple[str, UserModel]] = []
        self.deletes: list[str] = []

    async def changed(self, resource_id: str, update: UserModel) -> None:
        """Upsert dummy"""
        self.upserts.append((resource_id, update))

    async def deleted(self, resource_id: str) -> None:
        """Delete dummy"""
        self.deletes.append(resource_id)


class OutboxOrderTranslator(OutboxUserTranslator):
    """A translator class to subscribe to OrderModel events"""

    dto_model = OrderModel  # type: ignore[assignment]
    event_topic = "orders"


class DummyEventSubTranslator(EventSubscriberProtocol):
    """A translator class to subscribe to `TestEvent`, `TestEvent2` events"""

    types_of_interest = ["test_event", "test_event2"]
    topics_of_interest = ["test-events"]

    def __init__(self):
        self.consumed: list[tuple[dict, Ascii, Ascii, Ascii]] = []

    async def _consume_validated(  # type: ignore
        self, *, payload: dict, type_: Ascii, topic: Ascii, key: Ascii
    ) -> None:
        self.consumed.append((payload, type_, topic, key))


async def test_merged_translators(kafka: KafkaFixture):
    """Test combining outbox sub translators and non-outbox sub translators in
    the KafkaEventSubscriber, where the two varieties consume from different topics.
    """
    config = kafka.config

    test_event = SomeEvent()
    test_event2 = SomeEvent2()
    user_event = UserModel()
    order_event = OrderModel()

    # Publish an event of each type
    await kafka.publish_event(
        payload=user_event.model_dump(),
        topic="users",
        type_="upserted",
        key=user_event.name,
    )
    await kafka.publish_event(
        payload=order_event.model_dump(),
        topic="orders",
        type_="upserted",
        key=order_event.order_id,
    )
    await kafka.publish_event(
        payload=test_event.model_dump(),
        topic="test-events",
        type_="test_event",
        key=test_event.my_cool_id,
    )
    await kafka.publish_event(
        payload=test_event2.model_dump(),
        topic="test-events",
        type_="test_event2",
        key=test_event2.my_other_id,
    )

    # Create the translators, then bundle them into one via the TranslatorConverter
    non_outbox_translator = DummyEventSubTranslator()
    user_translator = OutboxUserTranslator()
    order_translator = OutboxOrderTranslator()

    super_translator = TranslatorConverter(
        translators=[non_outbox_translator, user_translator, order_translator]
    )

    async with KafkaEventSubscriber.construct(
        config=config,
        translator=super_translator,
    ) as subscriber:
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)


async def test_merged_translators_retry_topic(kafka: KafkaFixture):
    """Test combining outbox sub translators and non-outbox sub translators in
    the KafkaEventSubscriber while consuming from the retry topic.
    """
    config_dict = kafka.config.model_dump()
    config_dict["kafka_enable_dlq"] = True
    config = KafkaConfig(**config_dict)

    test_event = SomeEvent()
    test_event2 = SomeEvent2()
    user_event = UserModel()
    order_event = OrderModel()

    # Publish an event of each type
    await kafka.publish_event(
        payload=user_event.model_dump(),
        topic=f"{config.service_name}-retry",
        type_="upserted",
        key=user_event.name,
        headers={"original_topic": "users"},
    )
    await kafka.publish_event(
        payload=order_event.model_dump(),
        topic=f"{config.service_name}-retry",
        type_="upserted",
        key=order_event.order_id,
        headers={"original_topic": "orders"},
    )
    await kafka.publish_event(
        payload=test_event.model_dump(),
        topic=f"{config.service_name}-retry",
        type_="test_event",
        key=test_event.my_cool_id,
        headers={"original_topic": "test-events"},
    )
    await kafka.publish_event(
        payload=test_event2.model_dump(),
        topic=f"{config.service_name}-retry",
        type_="test_event2",
        key=test_event2.my_other_id,
        headers={"original_topic": "test-events"},
    )

    non_outbox_translator = DummyEventSubTranslator()
    user_translator = OutboxUserTranslator()
    order_translator = OutboxOrderTranslator()

    super_translator = TranslatorConverter(
        translators=[non_outbox_translator, user_translator, order_translator]
    )

    async with KafkaEventSubscriber.construct(
        config=config, translator=super_translator, dlq_publisher=AsyncMock()
    ) as subscriber:
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)
        await subscriber.run(forever=False)

    # Test that all events are consumed just like they would be from the original topics
    assert len(non_outbox_translator.consumed) == 2
    assert [e[0] for e in non_outbox_translator.consumed] == [
        test_event.model_dump(),
        test_event2.model_dump(),
    ]
    assert user_translator.upserts == [(user_event.name, user_event)]
    assert order_translator.upserts == [(order_event.order_id, order_event)]

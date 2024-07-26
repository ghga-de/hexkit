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
"""Tests for the Dead Letter Queue (DLQ) event subscribers"""

from contextlib import nullcontext
from copy import deepcopy
from typing import Optional

import pytest
from pydantic import BaseModel

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.akafka.provider.daosub import KafkaOutboxSubscriber
from hexkit.providers.akafka.provider.eventsub import (
    ORIGINAL_TOPIC_FIELD,
    ExtractedEvent,
    KafkaDLQSubscriber,
    KafkaEventSubscriber,
    OriginalTopicError,
)
from hexkit.providers.akafka.testutils import (  # noqa: F401
    KafkaFixture,
    kafka_container_fixture,
    kafka_fixture,
)

TEST_EVENT = ExtractedEvent(
    payload={"key": "value"},
    type_="test_type",
    topic="test-topic",
    key="key",
)


class OutboxDto(BaseModel):
    """Dummy DTO model to use for outbox tests"""

    user_id: str
    email: str
    name: str


OUTBOX_EVENT_UPSERT = ExtractedEvent(
    payload=OutboxDto(
        user_id="123456", email="test@test.com", name="Test User"
    ).model_dump(),
    topic="users",
    type_="upserted",
    key="123456",
)

OUTBOX_EVENT_DELETE = deepcopy(OUTBOX_EVENT_UPSERT)
OUTBOX_EVENT_DELETE.type_ = "deleted"


class FailSwitchTranslator(EventSubscriberProtocol):
    """Event translator that can be set to fail or collect events."""

    fail: bool
    failures: list[ExtractedEvent]
    successes: list[ExtractedEvent]
    topics_of_interest: list[str]
    types_of_interest: list[str]

    def __init__(
        self,
        *,
        topics_of_interest: list[str],
        types_of_interest: list[str],
        fail: bool = False,
    ):
        self.fail = fail
        self.successes = []
        self.failures = []
        self.topics_of_interest = topics_of_interest
        self.types_of_interest = types_of_interest

    async def _consume_validated(
        self,
        *,
        payload: JsonObject,
        type_: str,
        topic: str,
        key: str,
    ) -> None:
        """Add event to failures or successful list depending on `fail`.

        Raises RuntimeError if `fail` is True.
        """
        event = ExtractedEvent(payload=payload, type_=type_, topic=topic, key=key)
        if self.fail:
            self.failures.append(event)
            raise RuntimeError("Destined to fail.")
        self.successes.append(event)


class FailSwitchOutboxTranslator(DaoSubscriberProtocol):
    """Translator for the outbox event."""

    event_topic: str = "users"
    dto_model: type[BaseModel] = OutboxDto
    fail: bool = False
    upsertions: list[ExtractedEvent]
    deletions: list[str]

    def __init__(self, *, fail: bool = False) -> None:
        self.upsertions = []
        self.deletions = []
        self.fail = fail

    async def changed(self, resource_id: str, update: OutboxDto) -> None:
        """Dummy"""
        self.upsertions.append(
            ExtractedEvent(
                payload=update.model_dump(),
                type_="upserted",
                topic=self.event_topic,
                key=resource_id,
            )
        )
        if self.fail:
            raise RuntimeError("Destined to fail.")

    async def deleted(self, resource_id: str) -> None:
        """Dummy"""
        self.deletions.append(resource_id)
        if self.fail:
            raise RuntimeError("Destined to fail.")


class DummyPublisher(EventPublisherProtocol):
    """Dummy class to intercept publishing"""

    published: list[ExtractedEvent]

    def __init__(self) -> None:
        self.published = []

    async def _publish_validated(
        self, *, payload: JsonObject, type_: Ascii, key: Ascii, topic: Ascii
    ) -> None:
        self.published.append(
            ExtractedEvent(payload=payload, type_=type_, topic=topic, key=key)
        )


def make_config(
    kafka_config: Optional[KafkaConfig] = None,
    *,
    retry_topic: str = "retry",
    dlq_topic: str = "dlq",
    max_retries: int = 0,
    enable_dlq: bool = True,
) -> KafkaConfig:
    """Convenience method to merge kafka fixture config with provided DLQ values."""
    return KafkaConfig(
        service_name=getattr(kafka_config, "service_name", "test"),
        service_instance_id=getattr(kafka_config, "service_instance_id", "test"),
        kafka_servers=getattr(kafka_config, "kafka_servers", ["localhost:9092"]),
        kafka_dlq_topic=dlq_topic,
        kafka_retry_topic=retry_topic,
        kafka_max_retries=max_retries,
        kafka_enable_dlq=enable_dlq,
    )


@pytest.mark.parametrize(
    "retry_topic, dlq_topic, max_retries, enable_dlq, error",
    [
        ("retry", "dlq", 0, True, False),
        ("retry", "dlq", 1, True, False),
        ("retry", "dlq", -1, True, True),
        ("retry", "retry", 0, True, True),
        ("retry", "retry", 0, False, True),
        ("", "", 0, False, False),
        ("", "dlq", 0, False, False),
        ("retry", "dlq", 0, False, False),
        ("retry", "", 0, True, True),
        ("", "dlq", 0, True, True),
        ("", "", 0, True, True),
    ],
)
def test_config_validation(
    retry_topic: str,
    dlq_topic: str,
    max_retries: int,
    enable_dlq: bool,
    error: bool,
):
    """Test for config validation.

    Errors should occur:
    1. Anytime max_retries is < 0
    2. If retry and dlq topics are the same (non-empty)
    3. If the DLQ is enabled but the topics are not set (either or both)
    """
    with pytest.raises(ValueError) if error else nullcontext():
        make_config(
            retry_topic=retry_topic,
            dlq_topic=dlq_topic,
            max_retries=max_retries,
            enable_dlq=enable_dlq,
        )


@pytest.mark.asyncio()
async def test_original_topic_is_preserved(kafka: KafkaFixture):
    """Ensure the original topic is preserved when it reaches the DLQ subscriber and
    when it comes back to the Retry subscriber.

    Consume a failing event, send to DLQ, consume from DLQ, send to Retry, consume from
    Retry, and check the original topic.
    """
    config = make_config(kafka.config)

    # Publish test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    # Create dummy translator and set it to auto-fail, then run the Retry subscriber
    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"], fail=True
    )
    assert not translator.successes
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_sub:
        assert not translator.failures
        await retry_sub.run(forever=False)

        # Run the DLQ subscriber, telling it to publish the event to the retry topic
        async with KafkaDLQSubscriber.construct(
            config=config, publisher=kafka.publisher
        ) as dlq_sub:
            await dlq_sub.run()

        # Make sure the translator has nothing in the successful list, then run again
        assert not translator.successes
        translator.fail = False
        await retry_sub.run(forever=False)

    # Make sure the event received by the translator is identical to the original
    # This means the original topic is preserved and `_original_topic` is removed
    assert translator.failures == [TEST_EVENT]
    assert translator.successes == [TEST_EVENT]


@pytest.mark.parametrize("max_retries", [0, 1, 2])
@pytest.mark.asyncio()
async def test_max_retries_respected(kafka: KafkaFixture, max_retries: int):
    """Ensure the retry limit is respected."""
    config = make_config(kafka.config, max_retries=max_retries)

    # Publish test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    # Create dummy translator and set it to auto-fail, then run the Retry subscriber
    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"], fail=True
    )
    assert not translator.successes
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_sub:
        assert not translator.failures
        await retry_sub.run(forever=False)

    # Verify that the event was retried twice after initial failure
    assert translator.failures == [TEST_EVENT] * (max_retries + 1)


@pytest.mark.parametrize("max_retries", [0, 1, 2])
@pytest.mark.asyncio()
async def test_send_to_dlq(kafka: KafkaFixture, max_retries: int):
    """Ensure the event is sent to the DLQ topic when the retries are exhausted."""
    config = make_config(kafka.config, max_retries=max_retries)

    # Publish test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    # Set up dummies and consume the event
    dummy_publisher = DummyPublisher()
    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=dummy_publisher
    ) as retry_sub:
        translator.fail = True
        await retry_sub.run(forever=False)

    # Put together the expected event with the appended topic field
    modified_payload = {**TEST_EVENT.payload}
    modified_payload[ORIGINAL_TOPIC_FIELD] = TEST_EVENT.topic
    modified_event = ExtractedEvent(
        type_=TEST_EVENT.type_,
        topic=config.kafka_dlq_topic,
        key=TEST_EVENT.key,
        payload=modified_payload,
    )

    # Verify that the event was sent to the DLQ topic just once and that it has
    # the original topic field appended
    assert dummy_publisher.published == [modified_event]


@pytest.mark.asyncio()
async def test_send_to_retry(kafka: KafkaFixture):
    """Ensure the event is sent to the retry topic when the DLQ subscriber is instructed
    to do so.
    """
    config = make_config(kafka.config)

    # Publish event directly to DLQ Topic
    failed_event_payload = {"test_id": "123456", ORIGINAL_TOPIC_FIELD: "test-topic"}

    failed_event = ExtractedEvent(
        payload=failed_event_payload,
        type_="test_type",
        topic=config.kafka_dlq_topic,
        key="123456",
    )

    await kafka.publisher.publish(**vars(failed_event))

    # Set up dummies and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, publisher=dummy_publisher
    ) as dlq_sub:
        assert not dummy_publisher.published
        await dlq_sub.run(ignore=False)

    # Verify that the event was sent to the retry topic
    expected_event = ExtractedEvent(
        payload=failed_event_payload,
        type_="test_type",
        topic=config.kafka_retry_topic,
        key="123456",
    )

    assert dummy_publisher.published == [expected_event]


@pytest.mark.asyncio()
async def test_orig_topic_missing_dlq_sub(kafka: KafkaFixture):
    """Test for errors when the _original_topic is missing from an event."""
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEvent(
        payload={"test_id": "123456"},
        type_="test_type",
        topic=config.kafka_dlq_topic,
        key="key",
    )

    # Publish that event directly to DLQ Topic, as if it had already failed
    await kafka.publisher.publish(**vars(event))

    # Set up dummies and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, publisher=dummy_publisher
    ) as dlq_sub:
        assert not dummy_publisher.published

        # Expect the subscriber to raise an error because the _original_topic field is
        # missing
        with pytest.raises(RuntimeError):
            await dlq_sub.run(ignore=False)


@pytest.mark.asyncio()
async def test_orig_topic_missing_retry_sub(kafka: KafkaFixture):
    """Test for errors when the _original_topic is missing from an event consumed by
    a KafkaRetrySubscriber.
    """
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEvent(
        payload={"test_id": "123456"},
        type_="test_type",
        topic=config.kafka_retry_topic,
        key="key",
    )

    # Publish that event directly to DLQ Topic, as if it had already failed
    await kafka.publisher.publish(**vars(event))

    # Set up dummies and consume the event with the DLQ Subscriber
    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"]
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_subscriber:
        assert not translator.failures or translator.successes

        # Expect the subscriber to raise an error because the _original_topic field is
        # missing
        with pytest.raises(OriginalTopicError):
            await retry_subscriber.run(forever=False)


@pytest.mark.asyncio()
async def test_dlq_subscriber_ignore(kafka: KafkaFixture):
    """Test what happens when a DLQ Subscriber is instructed to ignore an event."""
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEvent(
        payload={"test_id": "123456", ORIGINAL_TOPIC_FIELD: "test-topic"},
        type_="test_type",
        topic=config.kafka_dlq_topic,
        key="key",
    )

    # Publish that event directly to DLQ Topic, as if it had already failed
    await kafka.publisher.publish(**vars(event))

    # Set up dummies and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, publisher=dummy_publisher
    ) as dlq_sub:
        assert not dummy_publisher.published
        await dlq_sub.run(ignore=True)

    # Assert that the event was not published to the retry topic
    assert not dummy_publisher.published


@pytest.mark.asyncio()
async def test_successful_event(kafka: KafkaFixture):
    """Happy path for a successful event."""
    config = make_config(kafka.config)

    # publish the test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"]
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_sub:
        assert not translator.successes
        await retry_sub.run(forever=False)
        assert translator.successes == [TEST_EVENT]


@pytest.mark.asyncio()
async def test_no_retries_no_dlq_original_error(kafka: KafkaFixture):
    """Test that not using the dlq and configuring 0 retries results in failures that
    propagate the underlying error to the provider.
    """
    config = make_config(kafka.config, enable_dlq=False)

    # publish the test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_sub:
        assert not translator.successes
        with pytest.raises(RuntimeError, match="Destined to fail."):
            await retry_sub.run(forever=False)
        assert not translator.successes
        assert translator.failures == [TEST_EVENT]


@pytest.mark.asyncio()
@pytest.mark.xfail(reason="RetriesExhaustedError is not raised unless max_retries > 0")
async def test_no_retries_no_dlq_retries_error(kafka: KafkaFixture):
    """Test that not using the dlq and configuring 0 retries does NOT raise a
    RetriesExhaustedError.
    """
    config = make_config(kafka.config, max_retries=1, enable_dlq=False)

    # publish the test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, publisher=kafka.publisher
    ) as retry_sub:
        assert not translator.successes
        with pytest.raises(KafkaEventSubscriber.RetriesExhaustedError):
            await retry_sub.run(forever=False)
        assert not translator.successes
        assert translator.failures == [TEST_EVENT]


@pytest.mark.parametrize("event_type", ["upserted", "deleted"])
@pytest.mark.asyncio()
async def test_outbox_with_dlq(kafka: KafkaFixture, event_type: str):
    """Ensure that the DLQ lifecycle works with the KafkaOutboxSubscriber."""
    config = make_config(kafka.config)

    translator = FailSwitchOutboxTranslator(fail=True)
    list_to_check = (
        translator.upsertions if event_type == "upserted" else translator.deletions
    )

    event = OUTBOX_EVENT_UPSERT if event_type == "upserted" else OUTBOX_EVENT_DELETE

    # publish the test event
    await kafka.publisher.publish(**vars(event))

    # Run the outbox subscriber and expect it to fail
    async with KafkaOutboxSubscriber.construct(
        config=config, publisher=kafka.publisher, translators=[translator]
    ) as outbox_sub:
        assert not list_to_check
        await outbox_sub.run(forever=False)
        assert list_to_check == [event] if event_type == "upserted" else [event.key]

        # Consume event from the DLQ topic, publish to retry topic
        async with KafkaDLQSubscriber.construct(
            config=config, publisher=kafka.publisher
        ) as dlq_sub:
            await dlq_sub.run()

        # Retry the event after clearing the list
        list_to_check.clear()  # type: ignore
        translator.fail = False
        assert not list_to_check
        await outbox_sub.run(forever=False)
        assert list_to_check == [event] if event_type == "upserted" else [event.key]
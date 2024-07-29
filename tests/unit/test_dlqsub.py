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
    ExtractedEventInfo,
    KafkaDLQSubscriber,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testutils import (  # noqa: F401
    KafkaFixture,
    kafka_container_fixture,
    kafka_fixture,
)
from tests.fixtures.utils import (
    assert_logged,
    assert_not_logged,
    caplog_debug_fixture,  # noqa: F401
)

TEST_EVENT = ExtractedEventInfo(
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


OUTBOX_EVENT_UPSERT = ExtractedEventInfo(
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
    failures: list[ExtractedEventInfo]
    successes: list[ExtractedEventInfo]
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
        if ORIGINAL_TOPIC_FIELD in payload:
            raise RuntimeError("Original topic field should not be present.")
        event = ExtractedEventInfo(payload=payload, type_=type_, topic=topic, key=key)
        if self.fail:
            self.failures.append(event)
            raise RuntimeError("Destined to fail.")
        self.successes.append(event)


class FailSwitchOutboxTranslator(DaoSubscriberProtocol):
    """Translator for the outbox event."""

    event_topic: str = "users"
    dto_model: type[BaseModel] = OutboxDto
    fail: bool = False
    upsertions: list[ExtractedEventInfo]
    deletions: list[str]

    def __init__(self, *, fail: bool = False) -> None:
        self.upsertions = []
        self.deletions = []
        self.fail = fail

    async def changed(self, resource_id: str, update: OutboxDto) -> None:
        """Dummy"""
        if ORIGINAL_TOPIC_FIELD in update.model_dump():
            raise RuntimeError("Original topic field should not be present.")
        self.upsertions.append(
            ExtractedEventInfo(
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

    published: list[ExtractedEventInfo]

    def __init__(self) -> None:
        self.published = []

    async def _publish_validated(
        self, *, payload: JsonObject, type_: Ascii, key: Ascii, topic: Ascii
    ) -> None:
        self.published.append(
            ExtractedEventInfo(payload=payload, type_=type_, topic=topic, key=key)
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
@pytest.mark.parametrize("enable_dlq", [True, False])
@pytest.mark.asyncio()
async def test_retries_exhausted(
    kafka: KafkaFixture, max_retries: int, enable_dlq: bool, caplog_debug
):
    """Ensure the event is sent to the DLQ topic when the retries are exhausted if
    the DLQ is enabled. If the DLQ is disabled, then the underlying error should be
    raised.
    """
    config = make_config(kafka.config, max_retries=max_retries, enable_dlq=enable_dlq)

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
        with pytest.raises(RuntimeError) if not enable_dlq else nullcontext():
            await retry_sub.run(forever=False)

    # Verify that the event was retried twice after initial failure
    assert translator.failures == [TEST_EVENT] * (max_retries + 1)

    # Check for initial failure log
    assert_logged(
        "WARNING",
        "Failed initial attempt to consume event of type 'test_type' on topic"
        + " 'test-topic' with key 'key'.",
        caplog_debug.records,
    )

    # Make sure we see the expected number of retry logs
    for n in range(1, max_retries + 1):
        assert_logged(
            "INFO",
            f"Retry {n} of {max_retries} for event of type 'test_type' on topic"
            + " 'test-topic' with key 'key'.",
            caplog_debug.records,
        )

    # Check for final retry-related log
    retry_log = f"All retries (total of {max_retries}) exhausted for 'test_type' event."
    if max_retries:
        assert_logged("WARNING", retry_log, caplog_debug.records)
    else:
        assert_not_logged("WARNING", retry_log, caplog_debug.records)

    # Put together the expected event with the original topic field appended
    failed_event_payload = {**TEST_EVENT.payload}
    failed_event_payload[ORIGINAL_TOPIC_FIELD] = TEST_EVENT.topic
    failed_event = ExtractedEventInfo(
        type_=TEST_EVENT.type_,
        topic=config.kafka_dlq_topic,
        key=TEST_EVENT.key,
        payload=failed_event_payload,
    )

    # Verify that the event was sent to the DLQ topic just once and that it has
    # the original topic field appended
    expected_published = [failed_event] if enable_dlq else []
    assert dummy_publisher.published == expected_published
    if enable_dlq:
        assert_logged(
            "INFO", "Published event to DLQ topic 'dlq'", caplog_debug.records
        )
    else:
        parsed_log = assert_logged(
            "CRITICAL",
            "An error occurred while processing event of type '%s': %s. It was NOT"
            + " placed in the DLQ topic (%s)",
            caplog_debug.records,
            parse=False,
        )
        assert parsed_log.endswith("(DLQ is disabled)")


@pytest.mark.asyncio()
async def test_send_to_retry(kafka: KafkaFixture, caplog_debug):
    """Ensure the event is sent to the retry topic when the DLQ subscriber is instructed
    to do so.
    """
    config = make_config(kafka.config)

    # Publish event directly to DLQ Topic
    failed_event_payload = {"test_id": "123456", ORIGINAL_TOPIC_FIELD: "test-topic"}

    failed_event = ExtractedEventInfo(
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

    assert_logged(
        "INFO",
        "Published an event with type 'test_type' to the retry topic 'retry'",
        caplog_debug.records,
    )

    # Verify that the event was sent to the retry topic
    expected_event = ExtractedEventInfo(
        payload=failed_event_payload,
        type_="test_type",
        topic=config.kafka_retry_topic,
        key="123456",
    )

    assert dummy_publisher.published == [expected_event]


@pytest.mark.asyncio()
async def test_consume_dlq_without_og_topic(kafka: KafkaFixture, caplog_debug):
    """Test for expected error when the _original_topic is missing from an event."""
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEventInfo(
        payload={"test_id": "123456"},
        type_="test_type",
        topic=config.kafka_dlq_topic,
        key="key",
    )

    # Publish that event directly to DLQ Topic, as if it had already failed
    await kafka.publisher.publish(**vars(event))

    # Set up dummy publisher and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, publisher=dummy_publisher
    ) as dlq_sub:
        # Expect to raise an error because the _original_topic field is missing
        text = r"Unable to get original topic from event: dlq - \d - key - \d"
        with pytest.raises(KafkaDLQSubscriber.OriginalTopicError, match=text) as err:
            await dlq_sub.run(ignore=False)
        assert not dummy_publisher.published

    assert_logged("CRITICAL", err.value.args[0], caplog_debug.records)


@pytest.mark.asyncio()
async def test_consume_retry_without_og_topic(kafka: KafkaFixture, caplog_debug):
    """If the original topic is missing when consuming an event from the retry queue,
    the event should be ignored and the offset committed. The information should be logged.
    """
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEventInfo(
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

        await retry_subscriber.run(forever=False)
        parsed_log = assert_logged(
            "INFO",
            "Ignored event of type '%s': %s, errors: %s",
            caplog_debug.records,
            parse=False,
        )
        assert parsed_log.startswith("Ignored event of type 'test_type': retry")
        assert parsed_log.endswith("errors: topic is empty")


@pytest.mark.asyncio()
async def test_dlq_subscriber_ignore(kafka: KafkaFixture, caplog_debug):
    """Test what happens when a DLQ Subscriber is instructed to ignore an event."""
    config = make_config(kafka.config)

    # make an event without the _original_topic field in the payload
    event = ExtractedEventInfo(
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

    parsed_log = assert_logged(
        "INFO",
        "Ignored event from DLQ topic '%s': %s",
        caplog_debug.records,
        parse=False,
    )
    assert parsed_log.startswith("Ignored event from DLQ topic 'dlq': dlq")

    # Assert that the event was not published to the retry topic
    assert not dummy_publisher.published


@pytest.mark.asyncio()
async def test_no_retries_no_dlq_original_error(kafka: KafkaFixture, caplog_debug):
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

    parsed_log = assert_logged(
        "CRITICAL",
        message="An error occurred while processing event of type '%s':"
        + " %s. It was NOT placed in the DLQ topic (%s)",
        records=caplog_debug.records,
        parse=False,
    )
    assert parsed_log.startswith(
        "An error occurred while processing event of type 'test_type':"
    )
    assert parsed_log.endswith("(DLQ is disabled)")

    assert_not_logged(
        "WARNING",
        "All retries (total of 0) exhausted for 'test_type' event.",
        caplog_debug.records,
    )


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


@pytest.mark.asyncio
async def test_kafka_event_subcriber_construction(caplog):
    """Test construction of the KafkaEventSubscriber, ensuring an error is raised if
    the dlq is enabled but no provider is used.
    """
    config = make_config()
    translator = FailSwitchTranslator(
        topics_of_interest=["test-topic"], types_of_interest=["test_type"]
    )

    with pytest.raises(ValueError):
        async with KafkaEventSubscriber.construct(config=config, translator=translator):
            assert False

    assert_logged(
        "ERROR",
        "A publisher is required when the DLQ is enabled.",
        caplog.records,
    )

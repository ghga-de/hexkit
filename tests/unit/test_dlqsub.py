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

from collections.abc import Mapping
from contextlib import nullcontext
from copy import copy
from typing import Optional
from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from hexkit.correlation import set_correlation_id
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.akafka.provider.daosub import KafkaOutboxSubscriber
from hexkit.providers.akafka.provider.eventsub import (
    EXC_CLASS_FIELD,
    EXC_MSG_FIELD,
    ORIGINAL_TOPIC_FIELD,
    ConsumerEvent,
    DLQProcessingError,
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

DEFAULT_SERVICE_NAME = "test_publisher"  # see KafkaConfig instance in akafka.testutils
TEST_TOPIC = "test-topic"
TEST_TYPE = "test_type"
TEST_DLQ_TOPIC = "test-topic.test_publisher-dlq"
TEST_RETRY_TOPIC = "test_publisher-retry"
TEST_EVENT = ExtractedEventInfo(
    payload={"key": "value"},
    type_=TEST_TYPE,
    topic=TEST_TOPIC,
    key="key",
)

TEST_CORRELATION_ID = "81e9b4f6-ed56-4779-94e2-421d7f286837"
TEST_DLQ_EVENT = ExtractedEventInfo(
    payload=TEST_EVENT.payload,
    type_=TEST_TYPE,
    topic=TEST_DLQ_TOPIC,
    key="123456",
    headers={
        "correlation_id": TEST_CORRELATION_ID,
        EXC_CLASS_FIELD: "RuntimeError",
        EXC_MSG_FIELD: "Destined to fail.",
    },
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

OUTBOX_EVENT_DELETE = copy(OUTBOX_EVENT_UPSERT)
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
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        key: Ascii,
        topic: Ascii,
        headers: Mapping[str, str],
    ) -> None:
        self.published.append(
            ExtractedEventInfo(
                payload=payload, type_=type_, topic=topic, key=key, headers=headers
            )
        )


def make_config(
    kafka_config: Optional[KafkaConfig] = None,
    *,
    max_retries: int = 0,
    enable_dlq: bool = True,
    retry_backoff: int = 0,
) -> KafkaConfig:
    """Convenience method to merge kafka fixture config with provided DLQ values."""
    return KafkaConfig(
        service_name=getattr(kafka_config, "service_name", DEFAULT_SERVICE_NAME),
        service_instance_id=getattr(kafka_config, "service_instance_id", "test"),
        kafka_servers=getattr(kafka_config, "kafka_servers", ["localhost:9092"]),
        kafka_max_retries=max_retries,
        kafka_enable_dlq=enable_dlq,
        kafka_retry_backoff=retry_backoff,
    )


@pytest.mark.parametrize("max_retries", [-1, 0, 1])
def test_config_validation(max_retries: int):
    """Test for config validation.

    Errors should occur:
    1. Anytime max_retries is < 0
    """
    with pytest.raises(ValueError) if max_retries < 0 else nullcontext():
        make_config(max_retries=max_retries)


@pytest.mark.asyncio()
async def test_original_topic_is_preserved(kafka: KafkaFixture):
    """Ensure the original topic is preserved when it comes back to the subscriber.

    Consume a failing event, send to DLQ, consume from DLQ, send to Retry, consume from
    Retry, and check the original topic.
    """
    config = make_config(kafka.config)

    # Publish test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    # Create dummy translator and set it to auto-fail
    translator = FailSwitchTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    assert not translator.successes

    # Run the subscriber and expect it to fail, sending the event to the DLQ
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.failures
        await event_subscriber.run(forever=False)

        # Run the DLQ subscriber, telling it to publish the event to the retry topic
        async with KafkaDLQSubscriber.construct(
            config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=kafka.publisher
        ) as dlq_subscriber:
            await dlq_subscriber.process()

        # Make sure the translator has nothing in the successes list, then run again
        assert not translator.successes
        translator.fail = False
        await event_subscriber.run(forever=False)

    # Make sure the event received by the translator is identical to the original
    # This means the original topic is preserved and `original_topic` is removed
    assert translator.failures == [TEST_EVENT]
    assert translator.successes == [TEST_EVENT]


@pytest.mark.asyncio()
async def test_invalid_retries_left(kafka: KafkaFixture, caplog_debug):
    """Ensure that the proper error is raised when retries_left is invalid."""
    config = make_config(kafka.config, max_retries=2)
    translator = FailSwitchTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE]
    )
    dummy_publisher = DummyPublisher()
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=dummy_publisher
    ) as event_subscriber:
        with pytest.raises(KafkaEventSubscriber.RetriesLeftError):
            await event_subscriber._retry_event(event=TEST_EVENT, retries_left=-1)

        with pytest.raises(KafkaEventSubscriber.RetriesLeftError):
            await event_subscriber._retry_event(event=TEST_EVENT, retries_left=3)

    assert_logged(
        "ERROR",
        "Invalid value for retries_left: -1 (should be between 1 and 2, inclusive).",
        caplog_debug.records,
    )
    assert_logged(
        "ERROR",
        "Invalid value for retries_left: 3 (should be between 1 and 2, inclusive).",
        caplog_debug.records,
    )


@pytest.mark.parametrize("max_retries", [0, 1, 2])
@pytest.mark.parametrize("enable_dlq", [True, False])
@pytest.mark.asyncio()
async def test_retries_exhausted(
    kafka: KafkaFixture, max_retries: int, enable_dlq: bool, caplog_debug
):
    """Ensure the event is sent to the DLQ topic when the retries are exhausted if
    the DLQ is enabled. If the DLQ is disabled, then the underlying error should be
    raised instead.
    """
    config = make_config(
        kafka.config, max_retries=max_retries, enable_dlq=enable_dlq, retry_backoff=1
    )

    # Publish test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    # Set up dummies and consume the event
    dummy_publisher = DummyPublisher()
    translator = FailSwitchTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=dummy_publisher
    ) as event_subscriber:
        with pytest.raises(RuntimeError) if not enable_dlq else nullcontext():
            await event_subscriber.run(forever=False)

    # Verify that the event was retried "max_retries" times after initial failure (if any)
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
        backoff_time = config.kafka_retry_backoff * 2 ** (n - 1)
        assert_logged(
            "INFO",
            f"Retry {n} of {max_retries} for event of type 'test_type' on topic"
            + f" 'test-topic' with key 'key', beginning in {backoff_time} seconds.",
            caplog_debug.records,
        )

    # Check for final retry-related log
    retry_log = f"All retries (total of {max_retries}) exhausted for 'test_type' event."
    if max_retries:
        assert_logged("WARNING", retry_log, caplog_debug.records)
    else:
        assert_not_logged("WARNING", retry_log, caplog_debug.records)

    # Put together the expected event with the original topic field appended
    failed_event = ExtractedEventInfo(
        type_=TEST_EVENT.type_,
        topic=TEST_DLQ_TOPIC,
        key=TEST_EVENT.key,
        payload=TEST_EVENT.payload,
        headers={
            EXC_CLASS_FIELD: "RuntimeError",
            EXC_MSG_FIELD: "Destined to fail.",
        },
    )

    # Verify that the event was sent to the DLQ topic just once and that it has
    # the original topic field appended
    expected_published = [failed_event] if enable_dlq else []
    assert dummy_publisher.published == expected_published
    if enable_dlq:
        assert_logged(
            "INFO",
            f"Published event to DLQ topic '{TEST_DLQ_TOPIC}'",
            caplog_debug.records,
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
    to do so. This would occur in whatever service or app is resolving DLQ events.
    """
    config = make_config(kafka.config)

    dlq_event = copy(TEST_DLQ_EVENT)
    await kafka.publisher.publish(**vars(dlq_event))

    # Set up dummies and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=dummy_publisher
    ) as dlq_subscriber:
        assert not dummy_publisher.published
        await dlq_subscriber.process()

    assert_logged(
        "INFO",
        f"Published an event with type 'test_type' to the retry topic '{TEST_RETRY_TOPIC}'",
        caplog_debug.records,
    )

    # Verify that the event was sent to the RETRY topic
    dlq_event.topic = TEST_RETRY_TOPIC

    # The exc_... headers are not supposed to be in the retry event, but the original
    # topic should be!
    dlq_event.headers = {ORIGINAL_TOPIC_FIELD: TEST_TOPIC}
    assert EXC_CLASS_FIELD not in dlq_event.headers
    assert EXC_MSG_FIELD not in dlq_event.headers
    assert dummy_publisher.published == [dlq_event]


@pytest.mark.asyncio()
async def test_consume_retry_without_og_topic(kafka: KafkaFixture, caplog_debug):
    """If the original topic is missing when consuming an event from the retry queue,
    the event should be ignored and the offset committed. The information should be logged.
    """
    config = make_config(kafka.config)

    event = ExtractedEventInfo(
        payload={"test_id": "123456"},
        type_=TEST_TYPE,
        topic=TEST_RETRY_TOPIC,
        key="key",
    )

    # Publish that event directly to RETRY Topic, as if it had already been requeued,
    # the original topic header is intentionally not included here
    await kafka.publisher.publish(**vars(event))

    # Set up dummies and subscriber
    translator = FailSwitchTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE]
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.failures or translator.successes

        # Consume the event with the event subscriber
        await event_subscriber.run(forever=False)
        parsed_log = assert_logged(
            "INFO",
            "Ignored event of type '%s': %s, errors: %s",
            caplog_debug.records,
            parse=False,
        )
        assert parsed_log.startswith(
            f"Ignored event of type 'test_type': {TEST_RETRY_TOPIC}"
        )
        assert parsed_log.endswith("errors: topic is empty")


@pytest.mark.asyncio()
async def test_dlq_subscriber_ignore(kafka: KafkaFixture, caplog_debug):
    """Test what happens when a DLQ Subscriber is instructed to ignore an event."""
    config = make_config(kafka.config)

    # make an event without the original_topic field in the header
    event = ExtractedEventInfo(
        payload={"test_id": "123456"},
        type_=TEST_TYPE,
        topic=TEST_DLQ_TOPIC,
        key="key",
    )

    # Publish that event directly to DLQ Topic, as if it had already failed
    # the original topic header is not included at this point
    await kafka.publisher.publish(**vars(event))

    # Set up dummies and consume the event with the DLQ Subscriber
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=dummy_publisher
    ) as dlq_subscriber:
        assert not dummy_publisher.published
        await dlq_subscriber.ignore()

    parsed_log = assert_logged(
        "INFO",
        "Ignoring event from DLQ topic '%s': %s",
        caplog_debug.records,
        parse=False,
    )
    assert parsed_log.startswith(
        f"Ignoring event from DLQ topic '{TEST_DLQ_TOPIC}': test"
    )

    # Assert that the event was not published to the retry topic
    assert not dummy_publisher.published


@pytest.mark.asyncio()
async def test_no_retries_no_dlq_original_error(kafka: KafkaFixture, caplog_debug):
    """Test that not using the DLQ and configuring 0 retries results in failures that
    propagate the underlying error to the provider.
    """
    config = make_config(kafka.config, enable_dlq=False)

    # publish the test event
    await kafka.publisher.publish(**vars(TEST_EVENT))

    translator = FailSwitchTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.successes
        with pytest.raises(RuntimeError, match="Destined to fail."):
            await event_subscriber.run(forever=False)
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
        config=config, dlq_publisher=kafka.publisher, translators=[translator]
    ) as outbox_subscriber:
        assert not list_to_check
        await outbox_subscriber.run(forever=False)
        assert list_to_check == [event] if event_type == "upserted" else [event.key]

        # Consume event from the DLQ topic, publish to retry topic
        dlq_topic = f"users.{config.service_name}-dlq"
        async with KafkaDLQSubscriber.construct(
            config=config, dlq_topic=dlq_topic, dlq_publisher=kafka.publisher
        ) as dlq_subscriber:
            await dlq_subscriber.process()

        # Retry the event after clearing the list
        list_to_check.clear()
        translator.fail = False
        assert not list_to_check
        await outbox_subscriber.run(forever=False)
        assert list_to_check == [event] if event_type == "upserted" else [event.key]


@pytest.mark.asyncio
async def test_kafka_event_subscriber_construction(caplog):
    """Test construction of the KafkaEventSubscriber, ensuring an error is raised if
    the DLQ is enabled but no provider is used.
    """
    config = make_config()

    mock = Mock()
    mock.topics_of_interest = [TEST_TOPIC]

    with pytest.raises(ValueError):
        async with KafkaEventSubscriber.construct(config=config, translator=mock):
            pass

    assert_logged(
        "ERROR",
        "A publisher is required when the DLQ is enabled.",
        caplog.records,
    )


@pytest.mark.parametrize(
    "validation_error", [True, False], ids=["validation_error", "no_validation_error"]
)
@pytest.mark.asyncio
async def test_default_dlq_processor(
    kafka: KafkaFixture, caplog, validation_error: bool
):
    """Verify that `process_dlq_event` behaves as expected.

    Assert that the event is republished unchanged or ignored.
    """
    config = make_config(kafka.config)

    # Publish test event directly to DLQ with chosen correlation ID
    event = copy(TEST_DLQ_EVENT)
    async with set_correlation_id(TEST_CORRELATION_ID):
        if validation_error:
            event.type_ = ""
        await kafka.publish_event(**vars(event))

    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=dummy_publisher
    ) as dlq_subscriber:
        assert not dummy_publisher.published
        caplog.clear()
        await dlq_subscriber.process()
        assert dummy_publisher.published == [] if validation_error else [event]

    if validation_error:
        assert len(caplog.records) > 0  # could be more, but should be at least 1
        log = caplog.records[0]
        assert log.msg.startswith("Ignoring event from DLQ due to validation failure:")


@pytest.mark.parametrize(
    "processing_error", [True, False], ids=["processing_error", "no_processing_error"]
)
@pytest.mark.asyncio
async def test_custom_dlq_processors(kafka: KafkaFixture, processing_error: bool):
    """Test that a custom DLQ processor can be used with the KafkaDLQSubscriber."""

    class CustomDLQProcessor:
        hits: list[ConsumerEvent]
        fail: bool

        def __init__(self):
            self.hits = []
            self.fail = processing_error

        async def process(self, event: ConsumerEvent) -> Optional[ExtractedEventInfo]:
            self.hits.append(event)
            if self.fail:
                raise RuntimeError("Destined to fail.")
            return ExtractedEventInfo(event)

    config = make_config(kafka.config)

    # Publish test event directly to DLQ with chosen correlation ID
    async with set_correlation_id(TEST_CORRELATION_ID):
        await kafka.publisher.publish(**vars(TEST_DLQ_EVENT))

    # Create custom processor instance and consume with the KafkaDLQSubscriber
    custom_processor = CustomDLQProcessor()
    async with KafkaDLQSubscriber.construct(
        config=config,
        dlq_topic=TEST_DLQ_TOPIC,
        dlq_publisher=DummyPublisher(),
        process_dlq_event=custom_processor.process,
    ) as dlq_subscriber:
        assert not custom_processor.hits
        with pytest.raises(DLQProcessingError) if processing_error else nullcontext():
            await dlq_subscriber.process()

        # verify that the event was received processed by the custom processor
        assert len(custom_processor.hits)
        event = ExtractedEventInfo(custom_processor.hits[0])
        assert event == TEST_DLQ_EVENT


@pytest.mark.asyncio()
async def test_preview_repeatability(kafka: KafkaFixture):
    """Make sure the preview functionality works as expected.
    Also make sure that previewing events doesn't impact the offset of the
    next event that is consumed/processed.
    """
    config = make_config(kafka.config)

    async with set_correlation_id(TEST_CORRELATION_ID):
        # publish 3 events -- there will be logs about how we supplied correlation_id
        # but that doesn't matter and can be ignored
        await kafka.publisher.publish(**vars(TEST_DLQ_EVENT))
        event2 = copy(TEST_DLQ_EVENT)
        event3 = copy(TEST_DLQ_EVENT)
        event2.type_ = "test_type2"
        event3.type_ = "test_type3"
        await kafka.publisher.publish(**vars(event2))
        await kafka.publisher.publish(**vars(event3))

    # Set up the DLQ Subscriber and preview/process the events
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=dummy_publisher
    ) as dlq_sub:
        assert not dummy_publisher.published
        preview1 = await dlq_sub.preview(limit=2)

        # Make sure we got the first two events (because the limit is 2)
        assert len(preview1) == 2
        assert preview1 == [TEST_DLQ_EVENT, event2]

        # Make sure the preview is repeatable
        preview2 = await dlq_sub.preview(limit=2)
        assert preview2 == preview1 == [TEST_DLQ_EVENT, event2]

        # Process the first event
        await dlq_sub.process()
        assert len(dummy_publisher.published) == 1
        assert dummy_publisher.published[0].type_ == TEST_TYPE

        # Preview again -- we should get the last 2 events since the first was consumed
        preview3 = await dlq_sub.preview(limit=2)
        assert len(preview3) == 2
        assert preview3 == [event2, event3]


@pytest.mark.parametrize("event_published", [True, False])
@pytest.mark.asyncio()
async def test_preview_when_limit_exceeds_available_records(
    kafka: KafkaFixture, event_published: bool
):
    """Make sure the consumer doesn't hang when < `preview_limit` records are available."""
    config = make_config(kafka.config)

    if not event_published:
        async with set_correlation_id(TEST_CORRELATION_ID):
            await kafka.publisher.publish(**vars(TEST_DLQ_EVENT))

    # Set up the DLQ Subscriber and preview the events
    dummy_publisher = DummyPublisher()
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=dummy_publisher
    ) as dlq_sub:
        assert not dummy_publisher.published
        results = await dlq_sub.preview(limit=100)

        # Make sure we got something
        assert len(results) == 0 if event_published else 1
        assert results == [] if event_published else [TEST_DLQ_EVENT]


@pytest.mark.asyncio()
async def test_preview_pagination(kafka: KafkaFixture):
    """Test that the preview pagination works as expected."""
    config = make_config(kafka.config)

    # Publish an event to the dlq topic
    async with set_correlation_id(TEST_CORRELATION_ID):
        await kafka.publisher.publish(**vars(TEST_DLQ_EVENT))

    # Create and publish a second event to the second dlq topic with a different key
    dlq_event2 = copy(TEST_DLQ_EVENT)
    dlq_event2.key = "78910"
    async with set_correlation_id(TEST_CORRELATION_ID):
        await kafka.publisher.publish(**vars(dlq_event2))

    # spin up the DLQ subscribers and preview events
    async with KafkaDLQSubscriber.construct(
        config=config, dlq_topic=TEST_DLQ_TOPIC, dlq_publisher=DummyPublisher()
    ) as dlq_subscriber:
        results1 = await dlq_subscriber.preview()
        assert results1 == [TEST_DLQ_EVENT]

        results2 = await dlq_subscriber.preview(skip=1)
        assert results2 == [dlq_event2]

        results3 = await dlq_subscriber.preview(limit=100, skip=2)
        assert not results3


@pytest.mark.asyncio()
async def test_empty_dlq_timeout(kafka: KafkaFixture):
    """Test that fetching from an empty DLQ topic does not hang.

    The `process` and `ignore` methods of the KafkaDLQSubscriber should return
    after a brief timeout. The `preview` method should return an empty list.
    """
    config = make_config(kafka.config)

    async def custom_processor(event: ConsumerEvent) -> Optional[ExtractedEventInfo]:
        raise RuntimeError("This should not be called.")

    async with KafkaDLQSubscriber.construct(
        config=config,
        dlq_topic=TEST_DLQ_TOPIC,
        dlq_publisher=DummyPublisher(),
        process_dlq_event=custom_processor,
    ) as dlq_subscriber:
        assert await dlq_subscriber.preview() == []
        await dlq_subscriber.process()
        await dlq_subscriber.ignore()

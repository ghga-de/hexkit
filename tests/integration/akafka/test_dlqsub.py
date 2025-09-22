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
"""Tests for the Dead Letter Queue (DLQ) event subscribers"""

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime
from typing import Optional
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import UUID4, BaseModel

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.daosub import DaoSubscriberProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.eventsub import DLQSubscriberProtocol, EventSubscriberProtocol
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.akafka.provider.daosub import KafkaOutboxSubscriber
from hexkit.providers.akafka.provider.eventsub import (
    DLQEventInfo,
    ExtractedEventInfo,
    HeaderNames,
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

pytestmark = pytest.mark.asyncio()
DEFAULT_SERVICE_NAME = "test_publisher"  # see KafkaConfig instance in akafka.testutils
TEST_TOPIC = "test-topic"
TEST_TYPE = "test_type"

TEST_CORRELATION_ID = UUID("81e9b4f6-ed56-4779-94e2-421d7f286837")
TEST_EVENT_ID = UUID("d8dfea20-2581-4a29-b658-c14e20c7a2c5")
FRESH_DLQ_EVENT_ID = UUID("6c7489a8-e0cc-46b0-8539-90cf91218359")

TEST_EVENT = ExtractedEventInfo(
    payload={"key": "value"},
    type_=TEST_TYPE,
    topic=TEST_TOPIC,
    key="key",
    event_id=TEST_EVENT_ID,
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

OUTBOX_EVENT_DELETE = replace(OUTBOX_EVENT_UPSERT, type_="deleted")


class DummyTranslator(EventSubscriberProtocol):
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
        event_id: UUID4,
    ) -> None:
        """Add event to failures or successful list depending on `fail`.

        Raises RuntimeError if `fail` is True.
        """
        event = ExtractedEventInfo(
            payload=payload, type_=type_, topic=topic, key=key, event_id=event_id
        )
        if self.fail:
            self.failures.append(event)
            raise RuntimeError("Destined to fail.")
        self.successes.append(event)


class DummyOutboxTranslator(DaoSubscriberProtocol):
    """Translator for the outbox event that can be set to fail.

    Does not collect event information since it's not needed for testing.
    """

    event_topic: str = "users"
    dto_model: type[BaseModel] = OutboxDto
    fail: bool = False

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def _fail_if_needed(self) -> None:
        """Raise RuntimeError if `fail` is True."""
        if self.fail:
            raise RuntimeError("Destined to fail.")

    async def changed(self, resource_id: str, update: OutboxDto) -> None:
        """Dummy"""
        self._fail_if_needed()

    async def deleted(self, resource_id: str) -> None:
        """Dummy"""
        self._fail_if_needed()


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
        event_id: UUID4,
        topic: Ascii,
        headers: Mapping[str, str],
    ) -> None:
        self.published.append(
            ExtractedEventInfo(
                payload=payload,
                type_=type_,
                topic=topic,
                key=key,
                event_id=event_id,
                headers=headers,
            )
        )


class DLQTranslator(DLQSubscriberProtocol):
    """Translator implementing the DLQSubscriberProtocol"""

    def __init__(self):
        self.events: list[DLQEventInfo] = []

    async def _consume_validated(
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        topic: Ascii,
        key: Ascii,
        event_id: UUID4,
        timestamp: datetime,
        headers: Mapping[str, str],
    ) -> None:
        """Consumes a DLQ event and stores it in self.events"""
        event = DLQEventInfo(
            payload=payload,
            type_=type_,
            topic=topic,
            key=key,
            event_id=event_id,
            timestamp=timestamp,
            headers=headers,
        )
        self.events.append(event)


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
async def test_config_validation(max_retries: int):
    """Test for config validation.

    Errors should occur:
    1. Anytime max_retries is < 0
    """
    with pytest.raises(ValueError) if max_retries < 0 else nullcontext():
        make_config(max_retries=max_retries)


async def test_original_topic_is_preserved(kafka: KafkaFixture):
    """Ensure the original topic is preserved when it comes back to the subscriber.

    Consume a failing event, send to DLQ, consume from DLQ, send to Retry, consume from
    Retry, and check the original topic.
    """
    config = make_config(kafka.config)

    # Publish test event
    await kafka.publisher.publish(**TEST_EVENT.asdict())

    # Create dummy translator and set it to auto-fail
    translator = DummyTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    assert not translator.successes

    # Run the subscriber and expect it to fail, sending the event to the DLQ
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.failures
        await event_subscriber.run(forever=False)

    # Run the DLQ subscriber and check that the topic is TEST_TOPIC, not "dlq"
    dlq_translator = DLQTranslator()
    async with KafkaEventSubscriber.construct(
        config=config,
        translator=dlq_translator,
    ) as dlq_subscriber:
        await dlq_subscriber.run(forever=False)
        assert dlq_translator.events
        assert dlq_translator.events[0].topic == config.kafka_dlq_topic
        og_topic = dlq_translator.events[0].headers[HeaderNames.ORIGINAL_TOPIC]
        assert og_topic == TEST_TOPIC


async def test_invalid_retries_left(kafka: KafkaFixture, caplog_debug):
    """Ensure that the proper error is raised when retries_left is invalid."""
    config = make_config(kafka.config, max_retries=2)
    translator = DummyTranslator(
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
async def test_send_to_dlq_after_retries_exhausted(
    kafka: KafkaFixture, max_retries: int, enable_dlq: bool, caplog_debug
):
    """Ensure the event is sent to the DLQ topic when the retries are exhausted if
    the DLQ is enabled. If the DLQ is disabled, then the underlying error should be
    raised instead.

    This test checks the headers included with the DLQ event and verifies that a
    new value for the event_id is generated, while the original event ID is sent
    in the
    """
    config = make_config(
        kafka.config, max_retries=max_retries, enable_dlq=enable_dlq, retry_backoff=1
    )

    # Publish test event
    await kafka.publisher.publish(**TEST_EVENT.asdict())

    # Set up dummies and consume the event
    dummy_dlq_publisher = DummyPublisher()
    translator = DummyTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=dummy_dlq_publisher
    ) as event_subscriber:
        with pytest.raises(RuntimeError) if not enable_dlq else nullcontext():
            await event_subscriber.run(forever=False)

    # Verify that the event was retried "max_retries" times after initial failure (if any)
    assert translator.failures == [TEST_EVENT] * (max_retries + 1)

    # Check for initial failure log
    assert_logged(
        "WARNING",
        "Failed initial attempt to consume event. Topic=test-topic, type=test_type,"
        + f" key=key, event_id={TEST_EVENT_ID}.",
        caplog_debug.records,
    )

    # Make sure we see the expected number of retry logs
    for n in range(1, max_retries + 1):
        backoff_time = config.kafka_retry_backoff * 2 ** (n - 1)
        assert_logged(
            "INFO",
            f"Retry {n} of {max_retries} for event beginning in {backoff_time} seconds."
            + f" Topic=test-topic, type=test_type, key=key, event_id={TEST_EVENT_ID}.",
            caplog_debug.records,
        )

    # Check for final retry-related log
    retry_log = f"All retries (total of {max_retries}) exhausted for 'test_type' event."
    if max_retries:
        assert_logged("WARNING", retry_log, caplog_debug.records)
    else:
        assert_not_logged("WARNING", retry_log, caplog_debug.records)

    # If the DLQ is enabled, we expect the event to be published to the DLQ topic once
    assert len(dummy_dlq_publisher.published) == int(enable_dlq)

    # Verify that the event has the original topic, original event ID, and service name
    if enable_dlq:
        assert dummy_dlq_publisher.published
        published_event = dummy_dlq_publisher.published[0]
        published_headers = published_event.headers
        assert HeaderNames.ORIGINAL_EVENT_ID in published_headers
        assert published_headers[HeaderNames.ORIGINAL_EVENT_ID] == str(TEST_EVENT_ID)
        assert HeaderNames.ORIGINAL_TOPIC in published_headers
        assert published_headers[HeaderNames.ORIGINAL_TOPIC] == TEST_TOPIC
        assert HeaderNames.SERVICE_NAME in published_headers
        assert published_headers[HeaderNames.SERVICE_NAME] == config.service_name

        dlq_event_id = published_event.event_id
        assert dlq_event_id != TEST_EVENT_ID  # should have new ID for DLQ

        assert_logged(
            "INFO",
            f"Published event to DLQ topic 'dlq'. DLQ event_id={dlq_event_id},"
            + f" original_event_id={TEST_EVENT_ID}.",
            caplog_debug.records,
            parse=True,
        )
    else:
        assert_logged(
            "CRITICAL",
            "Failed to process event. It was NOT placed in the DLQ topic"
            + " (DLQ is disabled). Topic=test-topic, type=test_type, key=key,"
            + f" event_id={TEST_EVENT_ID}.",
            caplog_debug.records,
            parse=True,
        )


async def test_consume_retry_without_og_topic(kafka: KafkaFixture, caplog_debug):
    """If the original topic is missing when consuming an event from the retry queue,
    the event should be ignored and the offset committed. The information should be logged.
    """
    config = make_config(kafka.config)

    # Publish that event directly to RETRY Topic, as if it had already been requeued,
    # the original topic header is intentionally not included here
    retry_topic = f"retry-{config.service_name}"
    event = replace(TEST_EVENT, topic=retry_topic)
    await kafka.publisher.publish(**event.asdict())

    # Set up dummies and subscriber
    translator = DummyTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE]
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.failures or translator.successes

        # Consume the event with the event subscriber
        await event_subscriber.run(forever=False)

    assert_logged(
        "INFO",
        f"Ignored event. Topic={retry_topic}, type={event.type_}, key={event.key},"
        + f" event_id={event.event_id}, errors: topic is empty.",
        caplog_debug.records,
        parse=True,
    )


async def test_no_retries_no_dlq_original_error(kafka: KafkaFixture, caplog_debug):
    """Test that not using the DLQ and configuring 0 retries, errors are propagated
    to the provider.
    """
    config = make_config(kafka.config, enable_dlq=False)

    # publish the test event
    await kafka.publisher.publish(**TEST_EVENT.asdict())

    translator = DummyTranslator(
        topics_of_interest=[TEST_TOPIC], types_of_interest=[TEST_TYPE], fail=True
    )
    async with KafkaEventSubscriber.construct(
        config=config, translator=translator, dlq_publisher=kafka.publisher
    ) as event_subscriber:
        assert not translator.successes
        with pytest.raises(RuntimeError, match=r"Destined to fail\."):
            await event_subscriber.run(forever=False)
        assert not translator.successes
        assert translator.failures == [TEST_EVENT]

    assert_logged(
        "CRITICAL",
        message="Failed to process event. It was NOT placed in the DLQ topic"
        + " (DLQ is disabled). Topic=test-topic, type=test_type,"
        + f" key=key, event_id={TEST_EVENT_ID}.",
        records=caplog_debug.records,
        parse=True,
    )


@pytest.mark.parametrize("upserted", [True, False], ids=["Upserted", "Deleted"])
async def test_outbox_with_dlq(kafka: KafkaFixture, upserted: bool):
    """Ensure that the DLQ lifecycle works with the KafkaOutboxSubscriber."""
    config = make_config(kafka.config)
    expected_event = OUTBOX_EVENT_UPSERT if upserted else OUTBOX_EVENT_DELETE

    # publish the test event
    await kafka.publisher.publish(**expected_event.asdict())

    # Run the outbox subscriber and expect it to fail (sending event to DLQ topic)
    async with KafkaOutboxSubscriber.construct(
        config=config,
        dlq_publisher=kafka.publisher,
        translators=[DummyOutboxTranslator(fail=True)],
    ) as outbox_subscriber:
        await outbox_subscriber.run(forever=False)

        # Consume event from the DLQ topic with a DLQ sub translator and check for match
        dlq_translator = DLQTranslator()
        async with KafkaEventSubscriber.construct(
            config=config, translator=dlq_translator, dlq_publisher=kafka.publisher
        ) as dlq_subscriber:
            await dlq_subscriber.run(forever=False)
            assert dlq_translator.events
            dlq_event = dlq_translator.events[0]
            assert dlq_event.payload == expected_event.payload
            assert dlq_event.type_ == expected_event.type_
            assert dlq_event.topic == config.kafka_dlq_topic
            assert dlq_event.headers[HeaderNames.EXC_CLASS] == "RuntimeError"
            assert dlq_event.headers[HeaderNames.EXC_MSG] == "Destined to fail."


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

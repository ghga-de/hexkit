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
"""Test to verify correlation ID functionality."""

import asyncio
import random
from collections import namedtuple
from contextlib import nullcontext
from contextvars import ContextVar
from unittest.mock import AsyncMock, Mock

import pytest

from hexkit.correlation import (
    CorrelationIdContextError,
    InvalidCorrelationIdError,
    correlation_id_var,
    get_correlation_id,
    set_correlation_id,
    validate_correlation_id,
)
from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka.provider import KafkaConfig, KafkaEventSubscriber
from hexkit.providers.akafka.testutils import (
    KafkaFixture,
    get_kafka_fixture,
)
from hexkit.providers.testing.utils import get_event_loop
from hexkit.utils import set_context_var

# Set seed to avoid non-deterministic outcomes with random.random()
random.seed(17)

VALID_CORRELATION_ID = "7041eb31-7333-4b57-97d7-90f5562c3383"

event_loop = get_event_loop("module")
kafka_fixture = get_kafka_fixture("module")


async def set_id_sleep_resume(correlation_id: str):
    """An async task to set the correlation ID ContextVar and yield control temporarily
    back to the event loop before resuming.
    Test with a sleep time of 0-2s and a random combination of context
    manager/directly setting ContextVar.
    """
    use_context_manager = random.choice((True, False))
    if use_context_manager:
        async with set_context_var(correlation_id_var, correlation_id):
            await asyncio.sleep(random.random() * 2)  # Yield control to the event loop
            # Check if the correlation ID is still the same
            assert correlation_id_var.get() == correlation_id, "Correlation ID changed"

        # make sure value is reset after exiting context manager
        assert correlation_id_var.get() == ""
    else:
        token = correlation_id_var.set(correlation_id)  # Set correlation ID for task
        await asyncio.sleep(random.random() * 2)  # Yield control to the event loop
        # Check if the correlation ID is still the same
        assert correlation_id_var.get() == correlation_id, "Correlation ID changed"
        correlation_id_var.reset(token)


@pytest.mark.asyncio
async def test_correlation_id_isolation():
    """Make sure correlation IDs are isolated to the respective async task and that
    there's no interference from task switching.
    """
    tasks = [set_id_sleep_resume(f"test_{n}") for n in range(100)]
    await asyncio.gather(*tasks)


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("BAD_ID", InvalidCorrelationIdError),
        ("", InvalidCorrelationIdError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_correlation_id_validation(correlation_id: str, exception):
    """Ensure an error is raised when correlation ID validation fails."""
    with pytest.raises(exception) if exception else nullcontext():
        validate_correlation_id(correlation_id)


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("12345", InvalidCorrelationIdError),
        ("", InvalidCorrelationIdError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_set_correlation_id(correlation_id: str, exception):
    """Ensure correct error is raised when passing an invalid or empty string to
    `set_correlation_id`.
    """
    with pytest.raises(exception) if exception else nullcontext():
        async with set_correlation_id(correlation_id=correlation_id):
            pass


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("12345", InvalidCorrelationIdError),
        ("", CorrelationIdContextError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_get_correlation_id(correlation_id: str, exception):
    """Ensure an error is raised when calling `get_correlation_id` for an empty id or
    invalid ID.
    """
    async with set_context_var(correlation_id_var, correlation_id):
        with pytest.raises(exception) if exception else nullcontext():
            get_correlation_id()


@pytest.mark.asyncio
async def test_context_var_setter():
    """Make sure `set_context_var()` properly resets the context var after use."""
    default = "default"
    outer_value = "outer"
    inner_value = "inner"
    test_var: ContextVar[str] = ContextVar("test", default=default)

    # Make sure the initial `get()` returns the default value
    assert test_var.get() == default

    # Ensure the value is set in the context manager
    async with set_context_var(test_var, outer_value):
        assert test_var.get() == outer_value

        # Ensure the value that is reset is actually the previous value, not just default
        async with set_context_var(test_var, inner_value):
            assert test_var.get() == inner_value
        assert test_var.get() == outer_value

    # Ensure the set value is removed/cleaned up by the function
    assert test_var.get() == default


@pytest.mark.parametrize(
    "correlation_id,generate_correlation_id,expected_exception",
    [
        (VALID_CORRELATION_ID, False, None),
        (VALID_CORRELATION_ID, True, None),
        ("invalid", False, InvalidCorrelationIdError),
        ("invalid", True, InvalidCorrelationIdError),
        ("", False, CorrelationIdContextError),
        ("", True, None),
    ],
    ids=[
        "valid_id_without_generate_flag",
        "valid_id_with_generate_flag",
        "invalid_id_without_generate_flag",
        "invalid_id_with_generate_flag",
        "no_id_without_generate_flag",
        "no_id_with_generate_flag",
    ],
)
@pytest.mark.asyncio
async def test_correlation_publishing(
    kafka_fixture: KafkaFixture,
    correlation_id: str,
    generate_correlation_id: bool,
    expected_exception,
):
    """Test situations with event publishing using the correlation ID."""
    # Update configuration of publishing provider (KafkaEventPublisher).
    kafka_fixture.publisher._generate_correlation_id = generate_correlation_id
    assert kafka_fixture.publisher._generate_correlation_id == generate_correlation_id

    async with set_context_var(correlation_id_var, correlation_id):
        with pytest.raises(expected_exception) if expected_exception else nullcontext():
            await kafka_fixture.publish_event(
                payload={},
                type_="test_type",
                topic="test_topic",
                key="test_key",
            )


@pytest.mark.parametrize(
    "expected_correlation_id,cid_in_header,exception",
    [
        (VALID_CORRELATION_ID, True, None),
        (VALID_CORRELATION_ID, False, None),
        ("invalid", True, InvalidCorrelationIdError),
        ("invalid", False, None),
        ("", True, InvalidCorrelationIdError),
        ("", False, None),
    ],
)
@pytest.mark.asyncio
async def test_correlation_consuming(
    expected_correlation_id: str,
    cid_in_header: bool,
    exception,
):
    """Verify the logic in the Kafka consumer provider that retrieves and sets the
    correlation ID.
    Uses a mock because event consumption with bad data can only be tested by
    bypassing the publisher validation.
    """
    service_name = "event_subscriber"
    topic = "test_topic"
    type_ = "test_type"
    payload: JsonObject = {"test": "Hello World!"}
    headers: list[tuple[str, bytes]] = [("type", b"test_type")]

    # include the correlation ID header if the test case calls for it
    if cid_in_header:
        headers.append(
            ("correlation_id", bytes(expected_correlation_id, encoding="ascii"))
        )

    # establish a mock event object:
    Event = namedtuple(
        "Event",
        ["topic", "key", "value", "headers", "partition", "offset"],
    )
    event = Event(
        key="test_key",
        headers=headers,
        value=payload,
        topic=topic,
        partition=1,
        offset=0,
    )

    # create kafka consumer mock to inject the event object:
    consumer = AsyncMock()
    consumer.__anext__.return_value = event
    consumer_cls = Mock()
    consumer_cls.return_value = consumer

    # create protocol-compatible translator mock:
    config = KafkaConfig(  # type: ignore
        service_name=service_name,
        service_instance_id="1",
        kafka_servers=["my-fake-kafka-server"],
    )

    class TestTranslator(EventSubscriberProtocol):
        """Test class used to confirm that the `KafkaEventSubscriber` class gets and
        sets the correlation ID properly before passing the event data to the translator.
        """

        def __init__(
            self,
            config: KafkaConfig,
        ):
            """Initialize with config parameters."""
            self._config = config
            self.topics_of_interest = [topic]
            self.types_of_interest = [type_]

        async def _consume_validated(
            self, *, payload: JsonObject, type_: Ascii, topic: Ascii
        ) -> None:
            # Make sure the IDs match
            if not cid_in_header:
                assert False, "Translator called but event should have been ignored"
            assert get_correlation_id() == expected_correlation_id

    # Instantiate and run the consumer
    test_translator = TestTranslator(config=config)
    async with KafkaEventSubscriber.construct(
        config=config, translator=test_translator, kafka_consumer_cls=consumer_cls
    ) as kafka_event_subscriber:
        with pytest.raises(exception) if exception else nullcontext():
            await kafka_event_subscriber.run(forever=False)

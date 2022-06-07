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

"""Testing the in-memory publisher."""

import pytest

from hexkit.providers.testing.eventpub import (
    InMemEventPublisher,
    InMemEventStore,
    TopicExhaustedError,
)


@pytest.mark.asyncio
async def test_in_mem_publisher():
    """Test the InMemEventPublisher testing utilities."""

    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"
    payload = {"test_content": "Hello World"}

    # create an in memory event store:
    event_store = InMemEventStore()

    # create event publisher:
    event_publisher = InMemEventPublisher(event_store=event_store)

    # publish event using the provider:
    await event_publisher.publish(
        payload=payload,
        type_=type_,
        key=key,
        topic=topic,
    )

    # check if producer was correctly used:
    stored_event = event_store.get(topic)
    assert stored_event.key == key
    assert stored_event.payload == payload
    assert stored_event.type_ == type_


@pytest.mark.asyncio
async def test_in_mem_publisher_with_multiple_events():
    """
    Test that events are passed in the right order. And make sure that
    TopicExhaustionError is thrown when requesting more events from the event store
    then submitted.
    Moreover, it's tested that not passing an existing event store to the
    InMemEventPublisher (as opposed to the test `test_in_mem_publisher`) is working, too.
    """
    payloads = [
        {"test_content": "Hello"},
        {"test_content": "World"},
    ]
    type_, key, topic = "test_type", "test_key", "test_topic"
    event_publisher = InMemEventPublisher()
    event_store = event_publisher.event_store

    # publish all events first:
    for payload in payloads:
        await event_publisher.publish(
            payload=payload, type_=type_, key=key, topic=topic
        )

    # thereafter, check order in the event store:
    for payload in payloads:
        assert event_store.get(topic).payload is payload

    # request one more event:
    with pytest.raises(TopicExhaustedError):
        _ = event_store.get(topic)


@pytest.mark.asyncio
async def test_in_mem_publisher_with_multiple_topics():
    """Test that events are passed in the right order."""
    payload_per_topic = {
        "topic_1": {"test_content": "Hello"},
        "topic_2": {"test_content": "World"},
    }
    type_, key = "test_type", "test_key"
    event_publisher = InMemEventPublisher()
    event_store = event_publisher.event_store

    # publish all events first:
    for topic, payload in payload_per_topic.items():
        await event_publisher.publish(
            payload=payload, type_=type_, key=key, topic=topic
        )

    # thereafter, check order in the event store:
    for topic, payload in payload_per_topic.items():
        assert event_store.get(topic).payload is payload
        with pytest.raises(TopicExhaustedError):
            _ = event_store.get(topic).payload


def test_event_store_get_from_empty_topic():
    """Test that consuming from non-used/empty topic raises the correct exception."""
    topic = "topic_that_has_no_events_yet", "test_type", "test_key"
    event_store = InMemEventStore()

    with pytest.raises(TopicExhaustedError):
        _ = event_store.get(topic).payload

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

"""Utilities for testing code that uses Kafka-based provider.

Please note, only use for testing purposes.
"""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
import math
from typing import AsyncGenerator, Generator, Optional

import pytest_asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import Ascii, JsonObject
from hexkit.providers.akafka.provider import (
    ConsumerEvent,
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
    get_event_type,
)


@dataclass(frozen=True)
class Event:
    """Used to describe events expected in a specific topic with a specific key.
    To be used with an instance of the EventRecorder class.
    """

    payload: JsonObject
    type_: Ascii


class EventRecorder:
    """Instances of this class can look at at specific topic and check for expected
    events occuring with a specified event key."""

    class NotStartedError(RuntimeError):
        """Raised when the recording has not been started yet but is required for the
        requested action."""

        def __init__(self):
            super().__init__("Event recording has not been started yet.")
            
    class ValidationError(RuntimeError):
        """Raised when the recorded events do not match the expectations."""

        def __init__(self, recorded_events: list[Event], expected_events: list[Event]):
            """Initialize the error with information on the recorded and
            expected_events."""

            message = ("The recorded events did not match the expectations." +
            " Events recorded: " + ", ".join(recorded_events) + 
            " - but expected: " + ",".join(expected_events)
            )

        def __init__(self):
            super().__init__("Event recording has not been started yet.")

    def __init__(
        self,
        *,
        consumer: AIOKafkaConsumer,
        topic: Ascii,
        with_key: Ascii,
        expect_events: list[Event],
    ):
        """Initialize with connection detials and by defining an expectation.
        The specified events with the specified key are expected in the exact order as
        defined in the list. Events with other keys are ignored.
        """

        self._consumer = consumer
        self._topic = topic
        self._key = with_key
        self._expected_events = expect_events

        self._starting_offsets: Optional[dict[str, int]] = None

    async def _get_current_offsets(self) -> dict[str, int]:
        """Returns a dictionary where the keys are partition IDs and the values are the
        current offsets in the corresponding partitions."""

        subscribed_partitions: list[str] = [
            topic_partition.partition for topic_partition in self._consumer.assignment()
        ]

        return {
            partition: await self._consumer.position(partition)
            for partition in subscribed_partitions
        }

    def _set_offsets(self, offsets: dict[str, int]):
        """Set partition offsets.

        Args:
            offsets: A dictionary with partition IDs as keys and offsets as values.
        """

        for partition in offsets:
            self._consumer.seek(
                partition=TopicPartition(topic=self._topic, partition=partition),
                offset=offsets[partition],
            )

    async def start_recording(self) -> None:
        """Start looking for the expected events from now on."""

        if self._starting_offsets is not None:
            raise RuntimeError(
                "Recording has already been started. Cannot restart. Please define a"
                + " new EventRecorder."
            )

        await self._consumer.seek_to_end()
        self._starting_offsets = await self._get_current_offsets()

    async def _count_events_since_start(self) -> int:
        """Determines how many events have been publish since the starting offset.
        Thereby, it sums over all partitions. The offset will not be change.
        """

        if self._starting_offsets is None:
            raise self.NotStartedError()

        # get the offsets of the latest available events:
        await self._consumer.seek_to_end()
        latest_offsets = await self._get_current_offsets()

        # reset the offsets to the starting positions:
        self._set_offsets(self._starting_offsets)

        # calculate the difference in offsets and sum up:
        partition_wise_counts = [
            latest_offsets[partition] - self._starting_offsets[partition]
            for partition in self._starting_offsets
        ]
        return sum(partition_wise_counts)

    async def _consume_event(self) -> ConsumerEvent:
        """Consume a single event and return it."""

        return await self._consumer.__anext__()

    async def _get_events_since_start(self) -> list[Event]:
        """Consume events since the starting offset."""

        event_count = await self._count_events_since_start()

        # consume all the available events (but not more which would lead to an infitive
        # waiting):
        raw_events = [await self._consume_event() for _ in range(event_count)]

        # discard all events that do not match the key of interest:
        return [
            Event(payload=raw_event.value, type_=get_event_type(raw_event))
            for raw_event in raw_events
            if raw_event.key == self._key
        ]

    async def stop_and_check(self) -> None:
        """Stop recording and check the recorded events against the expectation.

        Raises:
            EventsMissmatchError: When the recorded events do not match the expectation.
        """

        recorded_events = await self._get_events_since_start()

        # ignore events that are not in the expected events:
        events_of_interest = [
            event for event in recorded_events if event in self._expected_events
        ]

        # check the expectations:
        if events_of_interest != self._expected_events:



@dataclass(frozen=True)
class KafkaFixture:
    """Yielded by the `kafka_fixture` function"""

    def __init__(self, kafka_servers: list[str], publisher: KafkaEventPublisher):
        """Initialize with connection details and a ready-to-use publisher."""

        self.kafka_servers = kafka_servers
        self.publisher = publisher

    async def publish_event(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: Ascii = "test"
    ) -> None:
        """A convienience method to publish a test event."""

        await self.publisher.publish(payload=payload, type_=type_, key=key, topic=topic)

    @asynccontextmanager
    async def expect_events(
        events: list[Event], *, in_topic: Ascii, with_key: Ascii
    ) -> AsyncGenerator[EventRecorder, None, None]:
        """"""

        consumer = AIOKafkaConsumer(
            in_topic,
            bootstrap_servers=self.kafka_servers,
            client_id="__my_special_event_recorder__",
            group_id="__my_special_event_recorder__",
            auto_offset_reset="latest",
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
        )

        try:
            await consumer.start()
            yield EventRecorder(
                consumer=consumer,
                topic=in_topic,
                with_key=with_key,
                expect_events=events,
            )
        finally:
            await consumer.stop()


@pytest_asyncio.fixture
async def kafka_fixture() -> Generator[KafkaFixture, None, None]:
    """Pytest fixture for tests depending on the Kafka-base providers."""

    with KafkaContainer(image="confluentinc/cp-kafka:5.4.9-1-deb8") as kafka:
        kafka_servers = [kafka.get_bootstrap_server()]

        async with KafkaEventPublisher.construct(
            config=KafkaConfig(
                service_name="test_publisher",
                service_instance_id="001",
                kafka_servers=kafka_servers,
            )
        ) as publisher:
            yield KafkaFixture(kafka_servers=kafka_servers, publisher=publisher)

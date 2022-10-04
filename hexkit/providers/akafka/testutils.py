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
from dataclasses import dataclass
from typing import AsyncGenerator, Optional, Sequence

import pytest_asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import Ascii, JsonObject
from hexkit.providers.akafka.provider import (
    ConsumerEvent,
    KafkaConfig,
    KafkaEventPublisher,
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

    @dataclass(frozen=True)
    class RecorderState:
        """Captures the recorder state after beeing started.
        It contains the client object used to consume from Apache Kafka (`consumer`) as
        well as the consumer's offset(s) in the event log(s) when the recorder was
        started (starting_offsets).
        """

        consumer: AIOKafkaConsumer
        starting_offsets: dict[str, int]

    class ValidationError(RuntimeError):
        """Raised when the recorded events do not match the expectations."""

        def __init__(
            self, recorded_events: Sequence[Event], expected_events: Sequence[Event]
        ):
            """Initialize the error with information on the recorded and
            expected_events."""

            message = (
                "The recorded events did not match the expectations."
                + " Events recorded: "
                + ", ".join([str(event) for event in recorded_events])
                + " - but expected: "
                + ",".join([str(event) for event in expected_events])
            )
            super().__init__(message)

    def __init__(
        self,
        *,
        kafka_servers: list[str],
        topic: Ascii,
        with_key: Ascii,
        expect_events: Sequence[Event],
    ):
        """Initialize with connection detials and by defining an expectation.
        The specified events with the specified key are expected in the exact order as
        defined in the list. Events with other keys are ignored.
        """

        self._kafka_servers = kafka_servers
        self._topic = topic
        self._key = with_key
        self._expected_events = expect_events

        self._state: Optional["EventRecorder.RecorderState"] = None

    async def _get_current_offsets(self) -> dict[str, int]:
        """Returns a dictionary where the keys are partition IDs and the values are the
        current offsets in the corresponding partitions."""

        if self._state is None:
            raise self.NotStartedError()

        subscribed_partitions: list[str] = [
            topic_partition.partition
            for topic_partition in self._state.consumer.assignment()
        ]

        return {
            partition: await self._state.consumer.position(partition)
            for partition in subscribed_partitions
        }

    def _set_offsets(self, offsets: dict[str, int]):
        """Set partition offsets.

        Args:
            offsets: A dictionary with partition IDs as keys and offsets as values.
        """

        if self._state is None:
            raise self.NotStartedError()

        for partition in offsets:
            self._state.consumer.seek(
                partition=TopicPartition(topic=self._topic, partition=partition),
                offset=offsets[partition],
            )

    async def start_recording(self) -> None:
        """Start looking for the expected events from now on."""

        if self._state is not None:
            raise RuntimeError(
                "Recording has already been started. Cannot restart. Please define a"
                + " new EventRecorder."
            )

        consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=",".join(self._kafka_servers),
            client_id="__my_special_event_recorder__",
            group_id="__my_special_event_recorder__",
            auto_offset_reset="latest",
            key_deserializer=lambda event_key: event_key.decode("ascii"),
            value_deserializer=lambda event_value: json.loads(
                event_value.decode("ascii")
            ),
        )

        self._state = self.RecorderState(
            consumer=consumer, starting_offsets=await self._get_current_offsets()
        )

    async def _count_events_since_start(self) -> int:
        """Determines how many events have been publish since the starting offset.
        Thereby, it sums over all partitions. The offset will not be change.
        """

        if self._state is None:
            raise self.NotStartedError()

        # get the offsets of the latest available events:
        await self._state.consumer.seek_to_end()
        latest_offsets = await self._get_current_offsets()

        # reset the offsets to the starting positions:
        self._set_offsets(self._state.starting_offsets)

        # calculate the difference in offsets and sum up:
        partition_wise_counts = [
            latest_offsets[partition] - self._state.starting_offsets[partition]
            for partition in self._state.starting_offsets
        ]
        return sum(partition_wise_counts)

    async def _consume_event(self) -> ConsumerEvent:
        """Consume a single event and return it."""

        if self._state is None:
            raise self.NotStartedError()

        return await self._state.consumer.__anext__()

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

    async def stop_and_check(self, ignore_unexpected_events: bool = False) -> None:
        """Stop recording and check the recorded events against the expectation.

        Args:
            ignore_unexptected_events:
                If set to `True`, recorded events that are not expected will be ignored.
                If set to `False` all events must be expected and no additional events
                may be recorded. Defaults to `False`.

        Raises:
            EventsMissmatchError: When the recorded events do not match the expectation.
        """

        recorded_events = await self._get_events_since_start()

        if ignore_unexpected_events:
            # ignore events that are not in the expected events:
            recorded_events = [
                event for event in recorded_events if event in self._expected_events
            ]

        # check the expectations:
        if recorded_events != self._expected_events:
            raise self.ValidationError(
                recorded_events=recorded_events,
                expected_events=self._expected_events,
            )


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

    async def expect_events(
        self, events: Sequence[Event], *, in_topic: Ascii, with_key: Ascii
    ) -> EventRecorder:
        """Returns an EventRecorder object that can be used in a asnyc with block to
        records events with the specified key in the specified topic (on __aenter__) and
        check that they match the specified sequence of expected events (on __aexit__).
        """

        return EventRecorder(
            kafka_servers=self.kafka_servers,
            topic=in_topic,
            with_key=with_key,
            expect_events=events,
        )


@pytest_asyncio.fixture
async def kafka_fixture() -> AsyncGenerator[KafkaFixture, None]:
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

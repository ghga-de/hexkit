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

from contextlib import asynccontextmanager
import json
from dataclasses import dataclass
from typing import AsyncGenerator, Generator, Optional

import pytest_asyncio
from testcontainers.kafka import KafkaContainer
from aiokafka import AIOKafkaConsumer

from hexkit.providers.akafka.provider import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.custom_types import JsonObject, Ascii


@dataclass(frozen=True)
class ExpectedEvent:
    """Used to describe events expected in a specific topic with a specific key.
    To be used with an instance of the EventRecorder class.
    """

    payload: JsonObject
    type_: Ascii


class EventRecorder:
    """Instances of this class can look at at specific topic and check for expected
    events occuring with a specified event key."""

    def __init__(
        self,
        *,
        consumer: AIOKafkaConsumer,
        with_key: Ascii,
        expect_events: list[ExpectedEvent],
    ):
        """Initialize with connection detials and by defining an expectation.
        The specified events with the specified key are expected in the exact order as
        defined in the list. Events with other keys are ignored.
        """

        self._consumer = consumer
        self._key = with_key
        self._expected_events = expect_events

        self.recording_started = False
        self._offset_by_partition: Optional[dict[str, int]]

    async def start_recording(self) -> None:
        """Start looking for the expected events from now on."""

        if self.recording_started:
            raise RuntimeError(
                "Recording has already been started. Cannot restart. Please define a"
                + " new EventRecorder."
            )

        await self._consumer.seek_to_end()
        subscribed_partitions = self._consumer.assignment()
        self._offset_by_partition = {
            partition: await self._consumer.position(partition)
            for partition in subscribed_partitions
        }


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
    async def get_event_recorder(
        *, in_topic: Ascii, with_key: Ascii, expect_events: list[ExpectedEvent]
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
                consumer=consumer, with_key=with_key, expect_events=expect_events
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

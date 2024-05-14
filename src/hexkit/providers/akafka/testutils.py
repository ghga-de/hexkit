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
#

"""Utilities for testing code that uses Kafka-based provider.

Please note, only use for testing purposes.
"""

import json
from collections.abc import AsyncGenerator, Generator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Optional, Union

try:
    from typing import Self
except ImportError:  # Python < 3.11
    from typing_extensions import Self


import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import Ascii, JsonObject, PytestScope
from hexkit.providers.akafka import KafkaConfig
from hexkit.providers.akafka.provider import (
    ConsumerEvent,
    KafkaEventPublisher,
    get_header_value,
    headers_as_dict,
)
from hexkit.providers.akafka.testcontainer import DEFAULT_IMAGE as KAFKA_IMAGE

__all__ = [
    "KAFKA_IMAGE",
    "EventBase",
    "EventRecorder",
    "ExpectedEvent",
    "RecordedEvent",
    "ValidationError",
    "KafkaConfig",
    "KafkaContainerFixture",
    "KafkaEventPublisher",
    "KafkaFixture",
    "get_kafka_container_fixture",
    "get_clean_kafka_fixture",
    "get_persistent_kafka_fixture",
    "kafka_container_fixture",
    "clean_kafka_fixture",
    "persistent_kafka_fixture",
    "kafka_fixture",
]


@dataclass(frozen=True)
class EventBase:
    """Base for describing events expected and recorded by the EventRecorder class."""

    payload: JsonObject
    type_: Ascii


@dataclass(frozen=True)
class ExpectedEvent(EventBase):
    """Used to describe events expected in a specific topic using an EventRecorder.

    Please note, the key type is optional. If it is set to `None` (the default), the
    event key will be ignored when compared to the recording.
    """

    key: Optional[Ascii] = None


@dataclass(frozen=True)
class RecordedEvent(EventBase):
    """Used by the EventRecorder class to describe events recorded in a specific topic."""

    key: Ascii


class ValidationError(RuntimeError):
    """Raised when the recorded events do not match the expectations."""

    def __init__(
        self,
        recorded_events: Sequence[RecordedEvent],
        expected_events: Sequence[ExpectedEvent],
        details: str,
    ):
        """Initialize the error with information on the recorded and
        expected_events.
        """
        event_log = (
            " Events recorded: "
            + ", ".join([str(event) for event in recorded_events])
            + " - but expected: "
            + ",".join([str(event) for event in expected_events])
        )

        message = f"The recorded events did not match the expectations: {details}." + (
            f" {event_log}"
            if len(event_log) <= 1000
            else " (Not showing recorded events because the output log is too long.)"
        )
        super().__init__(message)


def check_recorded_events(
    recorded_events: Sequence[RecordedEvent], expected_events: Sequence[ExpectedEvent]
):
    """Check a sequence of recorded events against a sequence of expected events.
    Raises ValidationError in case of mismatches.
    """
    get_detailed_error = partial(
        ValidationError,
        recorded_events=recorded_events,
        expected_events=expected_events,
    )

    n_recorded_events = len(recorded_events)
    n_expected_events = len(expected_events)
    if n_recorded_events != n_expected_events:
        raise get_detailed_error(
            details=f"expected {n_expected_events} events but recorded {n_recorded_events}"
        )

    def get_field_mismatch_error(field, index):
        return get_detailed_error(
            details=f"the {field} of the recorded event no. {index+1}"
            " does not match the expectations"
        )

    for index, (recorded_event, expected_event) in enumerate(
        zip(recorded_events, expected_events)
    ):
        if recorded_event.payload != expected_event.payload:
            raise get_field_mismatch_error(field="payload", index=index)
        if recorded_event.type_ != expected_event.type_:
            raise get_field_mismatch_error(field="type", index=index)
        if expected_event.key is not None and recorded_event.key != expected_event.key:
            raise get_field_mismatch_error(field="key", index=index)


class EventRecorder:
    """Instances of this class can look at at specific topic and check for expected
    events occurring with a specified event key.
    """

    class NotStartedError(RuntimeError):
        """Raised when the recording has not been started yet but is required for the
        requested action.
        """

        def __init__(self):
            super().__init__("Event recording has not been started yet.")

    class InProgressError(RuntimeError):
        """Raised when the recording is still in progress but need to be stopped for
        the rested action.
        """

        def __init__(self):
            super().__init__(
                "Event recording has not been stopped yet and is still in progress."
            )

    def __init__(
        self,
        *,
        kafka_servers: list[str],
        topic: Ascii,
    ):
        """Initialize with connection details."""
        self._kafka_servers = kafka_servers
        self._topic = topic

        self._starting_offsets: Optional[dict[str, int]] = None
        self._recorded_events: Optional[Sequence[RecordedEvent]] = None

    def _assert_recording_stopped(self) -> None:
        """Assert that the recording has been stopped. Raises an InProgressError or a
        NotStartedError otherwise.
        """
        if self._recorded_events is None:
            if self._starting_offsets is None:
                raise self.NotStartedError()
            raise self.InProgressError()

    @property
    def recorded_events(self) -> Sequence[RecordedEvent]:
        """The recorded events. Only available after the recording has been stopped."""
        self._assert_recording_stopped()
        return self._recorded_events  # type: ignore

    async def _get_consumer_offsets(
        self, *, consumer: AIOKafkaConsumer
    ) -> dict[str, int]:
        """Returns a dictionary where the keys are partition IDs and the values are the
        current offsets in the corresponding partitions for the provided consumer.
        The provided consumer instance must have been started.
        """
        topic_partitions = [
            topic_partition
            for topic_partition in consumer.assignment()
            if topic_partition.topic == self._topic
        ]

        return {
            topic_partition.partition: await consumer.position(topic_partition)
            for topic_partition in topic_partitions
        }

    def _set_consumer_offsets(
        self, offsets: dict[str, int], *, consumer: AIOKafkaConsumer
    ):
        """Set partition offsets the provided consumer instance.

        Args:
            offsets:
                A dictionary with partition IDs as keys and offsets as values.
            consumer:
                A AIOKafkaConsumer client instance. The provided consumer instance must
                have been started.

        """
        if self._starting_offsets is None:
            raise self.NotStartedError()

        for partition in offsets:
            consumer.seek(
                partition=TopicPartition(topic=self._topic, partition=partition),
                offset=offsets[partition],
            )

    def _get_consumer(self) -> AIOKafkaConsumer:
        """Get an AIOKafkaConsumer."""
        return AIOKafkaConsumer(
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

    async def _count_events_since_start(self, *, consumer: AIOKafkaConsumer) -> int:
        """Given a consumer instance, determine how many events have been published
        since the starting offset. Thereby, sum over all partitions. This does not
        change the offset. The provided consumer instance must have been started.
        """
        if self._starting_offsets is None:
            raise self.NotStartedError()

        # get the offsets of the latest available events:
        await consumer.seek_to_end()
        latest_offsets = await self._get_consumer_offsets(consumer=consumer)

        # reset the offsets to the starting positions:
        self._set_consumer_offsets(self._starting_offsets, consumer=consumer)

        # calculate the difference in offsets and sum up:
        partition_wise_counts = [
            latest_offsets[partition] - self._starting_offsets[partition]
            for partition in self._starting_offsets
        ]
        return sum(partition_wise_counts)

    @staticmethod
    async def _consume_event(*, consumer: AIOKafkaConsumer) -> ConsumerEvent:
        """Consume a single event from a consumer instance and return it. The provided
        consumer instance must have been started.
        """
        return await consumer.__anext__()

    async def _get_events_since_start(
        self, *, consumer: AIOKafkaConsumer
    ) -> list[RecordedEvent]:
        """Consume events since the starting offset. The provided consumer instance must
        have been started.
        """
        event_count = await self._count_events_since_start(consumer=consumer)

        # consume all the available events (but no more, as this would lead to infinite
        # waiting):
        raw_events = [
            await self._consume_event(consumer=consumer) for _ in range(event_count)
        ]

        # discard all events that do not match the key of interest:
        return [
            RecordedEvent(
                payload=raw_event.value,
                type_=get_header_value("type", headers=headers_as_dict(raw_event)),
                key=raw_event.key,
            )
            for raw_event in raw_events
        ]

    async def start_recording(self) -> None:
        """Start looking for the expected events from now on."""
        if self._starting_offsets is not None:
            raise RuntimeError(
                "Recording has already been started. Cannot restart. Please define a"
                + " new EventRecorder."
            )

        consumer = self._get_consumer()
        await consumer.start()

        # The number of events to consume is determined by the difference between the
        # starting and final offsets. To avoid waiting for too many events (and thus
        # waiting forever), seek_to_end so only events published inside the context of
        # the event recorder are considered.
        await consumer.seek_to_end()

        try:
            self._starting_offsets = await self._get_consumer_offsets(consumer=consumer)
        finally:
            await consumer.stop()

    async def stop_recording(self) -> None:
        """Stop recording and collect the recorded events"""
        if self._starting_offsets is None:
            raise self.NotStartedError()

        consumer = self._get_consumer()
        await consumer.start()

        try:
            self._recorded_events = await self._get_events_since_start(
                consumer=consumer
            )
        finally:
            await consumer.stop()

    async def __aenter__(self) -> "EventRecorder":
        """Start recording when entering the context block."""
        await self.start_recording()
        return self

    async def __aexit__(self, error_type, error_val, error_tb):
        """Stop recording and check the recorded events against the expectation when
        exiting the context block.
        """
        await self.stop_recording()


class KafkaFixture:
    """A fixture with utility methods for tests that use Apache Kafka."""

    def __init__(
        self,
        *,
        config: KafkaConfig,
        kafka_servers: list[str],
        publisher: KafkaEventPublisher,
        cmd_exec_func: Callable[[str, bool], None],
    ):
        """Initialize with connection details and a ready-to-use publisher."""
        self.config = config
        self.kafka_servers = kafka_servers
        self.publisher = publisher
        self._cmd_exec_func = cmd_exec_func

    async def publish_event(
        self, *, payload: JsonObject, type_: Ascii, topic: Ascii, key: Ascii = "test"
    ) -> None:
        """A convenience method to publish a test event."""
        await self.publisher.publish(payload=payload, type_=type_, key=key, topic=topic)

    def record_events(self, *, in_topic: Ascii) -> EventRecorder:
        """Constructs an EventRecorder object that can be used in an async with block to
        record events in the specified topic upon __aenter__ and stops the recording
        upon __aexit__.
        """
        return EventRecorder(kafka_servers=self.kafka_servers, topic=in_topic)

    def _build_record_deletion_config(self, partitions: JsonObject) -> JsonObject:
        """Build the config required to run the kafka-delete-records script."""
        # The required JSON config has a schema specified as follows:
        deletion_config: dict[str, Union[list, int]] = {
            "partitions": [],  # {topic:str, partition: int, offset: int}
            "version": 1,
        }

        # Add the partition offset info for each topic
        for item in partitions:
            for partition in item["partitions"]:  # type: ignore
                deletion_config["partitions"].append(  # type: ignore
                    {
                        "topic": item["topic"],  # type: ignore
                        "partition": partition["partition"],  # type: ignore
                        "offset": -1,  # -1 instructs kafka to delete all records
                    }
                )

        return deletion_config

    def _build_record_deletion_command(self, delete_config: JsonObject) -> str:
        """Build the command string used to run kafka-delete-records.

        The configuration is dumped to a file with an echo command, and then the
        delete command is called using that file.
        """
        file_name = "record-deletion.json"
        json_data = json.dumps(delete_config)

        # Build the two command strings that write the config file and run the deletion
        echo_command = f"echo '{json_data}' > {file_name}"
        deletion_command = (
            "kafka-delete-records --bootstrap-server localhost:9092 "
            + f"--offset-json-file {file_name}"
        )
        return f"{echo_command} && {deletion_command}"

    async def _get_topic_description(
        self,
        topics: Optional[Union[list[str], str]] = None,
        exclude_internal: bool = True,
    ) -> JsonObject:
        """Get a decription of the given topic(s).

        If no topics are specified, all topics will be covered, except internal topics
        unless otherwise specified.
        """
        admin_client = AIOKafkaAdminClient(bootstrap_servers=self.kafka_servers)
        await admin_client.start()
        try:
            if topics is None:
                # if topics is None, the admin client gets all topics
                if exclude_internal:
                    topics = [
                        topic
                        for topic in await admin_client.list_topics()
                        if not topic.startswith("__")
                    ]
            elif isinstance(topics, str):
                topics = [topics]
            return await admin_client.describe_topics(topics)
        finally:
            await admin_client.close()

    async def clear_topics(
        self,
        topics: Optional[Union[str, list[str]]] = None,
        exclude_internal: bool = True,
    ):
        """Clear messages from given topic(s).

        If no topics are specified, all topics will be cleared, except internal topics
        unless otherwise specified.
        """
        # Get the description of the topics to be deleted
        partition_info = await self._get_topic_description(topics, exclude_internal)

        # Build the command line and then run it in a shell
        deletion_config = self._build_record_deletion_config(partition_info)
        command = self._build_record_deletion_command(deletion_config)

        self._cmd_exec_func(command, True)

    @asynccontextmanager
    async def expect_events(
        self, events: Sequence[ExpectedEvent], *, in_topic: Ascii
    ) -> AsyncGenerator[EventRecorder, None]:
        """Can be used in an async with block to record events in the specified topic
        (on __aenter__) and check that they match the specified sequence of expected
        events (on __aexit__).
        """
        async with self.record_events(in_topic=in_topic) as event_recorder:
            yield event_recorder

        check_recorded_events(
            recorded_events=event_recorder.recorded_events, expected_events=events
        )


class KafkaContainerFixture(KafkaContainer):
    """Kafka test container with configuration and command execution."""

    kafka_config: KafkaConfig

    def __init__(self, port: int = 9093, **kwargs: Any) -> None:
        """Initialize the container."""
        super().__init__(image=KAFKA_IMAGE, port=port, **kwargs)

    def __enter__(self) -> Self:
        """Enter the container context."""
        super().__enter__()
        kafka_servers = [self.get_bootstrap_server()]
        self.kafka_config = KafkaConfig(
            service_name="test_publisher",
            service_instance_id="001",
            kafka_servers=kafka_servers,
        )
        return self

    def wrapped_exec_run(self, command: str, run_in_shell: bool):
        """Run the given command in the kafka testcontainer.

        Args:
            - `command`: The full command to run.
            - `run_in_shell`: If True, will run the command in a shell.

        Raises:
            - `RuntimeError`: when the exit code returned by the command is not zero.
        """
        cmd = ["sh", "-c", command] if run_in_shell else command
        exit_code, output = self.get_wrapped_container().exec_run(cmd)

        if exit_code != 0:
            raise RuntimeError(f"result: {exit_code}, output: {output.decode('utf-8')}")


def _kafka_container_fixture() -> Generator[KafkaContainerFixture, None, None]:
    """Fixture function for getting a running Kafka test container."""
    with KafkaContainerFixture() as kafka_container:
        yield kafka_container


def get_kafka_container_fixture(
    scope: PytestScope = "session", name: str = "kafka_container"
):
    """Get a Kafka test container fixture with desired scope and name.

    By default, the session scope is used for Kafka test containers.
    """
    return pytest.fixture(_kafka_container_fixture, scope=scope, name=name)


kafka_container_fixture = get_kafka_container_fixture()


async def _persistent_kafka_fixture(
    kafka_container: KafkaContainerFixture,
) -> AsyncGenerator[KafkaFixture, None]:
    """Fixture function that gets a persistent Kafka fixture.

    The state of Kafka is not cleaned up by this function.
    """
    config = kafka_container.kafka_config
    async with KafkaEventPublisher.construct(config=config) as publisher:
        kafka_fixture = KafkaFixture(
            config=config,
            kafka_servers=config.kafka_servers,
            cmd_exec_func=kafka_container.wrapped_exec_run,
            publisher=publisher,
        )
        yield kafka_fixture


def get_persistent_kafka_fixture(scope: PytestScope = "function", name: str = "kafka"):
    """Get a Kafka fixture with desired scope and name.

    The state of the Kafka test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Kafka test container.
    """
    return pytest_asyncio.fixture(_persistent_kafka_fixture, scope=scope, name=name)


persistent_kafka_fixture = get_persistent_kafka_fixture()


async def _clean_kafka_fixture(
    kafka_container: KafkaContainerFixture,
) -> AsyncGenerator[KafkaFixture, None]:
    """Fixture function that gets a clean Kafka fixture.

    The clean state is achieved by clearing all Kafka topics upfront.
    """
    async for kafka_fixture in _persistent_kafka_fixture(kafka_container):
        await kafka_fixture.clear_topics()
        yield kafka_fixture


def get_clean_kafka_fixture(scope: PytestScope = "function", name: str = "kafka"):
    """Get a Kafka fixture with desired scope and name.

    The state of Kafka is reset by clearing all topics before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Kafka test container.
    """
    return pytest_asyncio.fixture(_clean_kafka_fixture, scope=scope, name=name)


kafka_fixture = clean_kafka_fixture = get_clean_kafka_fixture()

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

"""Testing Apache Kafka based providers."""

import uuid
from contextlib import nullcontext
from datetime import date, datetime, timezone
from os import environ
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import MessageSizeTooLargeError
from aiokafka.structs import TopicPartition
from pydantic import SecretStr

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testcontainer import KafkaSSLContainer
from hexkit.providers.akafka.testutils import (
    ExpectedEvent,
    KafkaFixture,
    kafka_container_fixture,  # noqa: F401
    kafka_fixture,  # noqa: F401
)

from ..fixtures.kafka_secrets import KafkaSecrets

pytestmark = pytest.mark.asyncio()


async def test_kafka_event_publisher(kafka: KafkaFixture):
    """Test the KafkaEventPublisher."""
    payload: JsonObject = {
        "test_str": "Hello World",
        "test_bool": True,
        "test_int": 42,
        "test_float": -21.5,
        "test_null": None,
        "test_uuid": uuid.uuid4(),
        "test_date": date.today(),
        "test_datetime_utc": datetime.now(timezone.utc),
        "test_array": [1, "two", 3],
        "test_object": {"content": "value"},
        "test_nested": {"values": [1], "nested_object": {"id": uuid.uuid4()}},
    }
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    config = KafkaConfig(
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )

    async with kafka.expect_events(
        events=[ExpectedEvent(payload=payload, type_=type_, key=key)],
        in_topic=topic,
    ):
        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            await event_publisher.publish(
                payload=payload,
                type_=type_,
                key=key,
                topic=topic,
            )


async def test_kafka_event_subscriber(kafka: KafkaFixture):
    """Test the KafkaEventSubscriber with mocked EventSubscriber."""
    payload = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    # create protocol-compatible translator mock:
    translator = AsyncMock()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    # publish one event:
    await kafka.publish_event(topic=topic, payload=payload, key=key, type_=type_)

    # setup the provider:
    config = KafkaConfig(
        service_name="event_subscriber",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )
    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
    ) as event_subscriber:
        # consume one event:
        await event_subscriber.run(forever=False)

    # check if the translator was called correctly:
    translator.consume.assert_awaited_once_with(
        payload=payload, type_=type_, topic=topic, key=key
    )


async def test_kafka_ssl(tmp_path: Path):
    """Test connecting to Kafka via SSL (TLS)."""
    hostname = environ.get("TC_HOST") or "localhost"

    secrets = KafkaSecrets(hostname=hostname)

    path = tmp_path / ".ssl"
    path.mkdir()

    (path / "ca.crt").open("w").write(secrets.ca_cert)
    (path / "client.crt").open("w").write(secrets.client_cert)
    (path / "client.key").open("w").write(secrets.client_key)

    payload: JsonObject = {"test_content": "Be aware... Connect with care"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    with KafkaSSLContainer(
        cert=secrets.broker_cert,
        key=secrets.broker_key,
        password=secrets.broker_pwd,
        trusted=secrets.ca_cert,
        client_auth="required",
    ) as kafka:
        kafka_servers = [kafka.get_bootstrap_server()]

        config = KafkaConfig(
            service_name="test_ssl",
            service_instance_id="1",
            kafka_servers=kafka_servers,
            kafka_security_protocol="SSL",
            kafka_ssl_cafile=str(path / "ca.crt"),
            kafka_ssl_certfile=str(path / "client.crt"),
            kafka_ssl_keyfile=str(path / "client.key"),
            kafka_ssl_password=SecretStr(secrets.client_pwd),
        )

        async with KafkaEventPublisher.construct(config=config) as event_publisher:
            await event_publisher.publish(
                payload=payload,
                type_=type_,
                key=key,
                topic=topic,
            )

        translator = AsyncMock()
        translator.topics_of_interest = [topic]
        translator.types_of_interest = [type_]

        async with KafkaEventSubscriber.construct(
            config=config,
            translator=translator,
        ) as event_subscriber:
            await event_subscriber.run(forever=False)

        translator.consume.assert_awaited_once_with(
            payload=payload, type_=type_, topic=topic, key=key
        )


async def test_consumer_commit_mode(kafka: KafkaFixture):
    """Verify the consumer implementation behavior matches expectations."""
    type_ = "test_type"
    topic = "test_topic"

    partition = TopicPartition(topic, 0)

    error_message = "Consumer crashed successfully."

    async def crash(*, payload: JsonObject, type_: Ascii, topic: Ascii, key: Ascii):
        """Drop-in replacement for patch testing the consume method."""
        raise ValueError(error_message)

    # prepare subscriber
    config = KafkaConfig(
        service_name="test_subscriber",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
    )

    # Everything that matters happens in the consumer, no proper subscriber is needed
    translator = AsyncMock(spec=EventSubscriberProtocol)
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    await kafka.publish_event(payload={"test_msg": "msg1"}, type_=type_, topic=topic)

    # save original for patching
    consume_function = translator.consume

    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
    ) as event_subscriber:
        # provide correct type information
        consumer = cast(AIOKafkaConsumer, event_subscriber._consumer)

        # crash consumer while processing an event
        translator.consume = crash
        with pytest.raises(ValueError, match=error_message):
            await event_subscriber.run(forever=False)

        # assert event was not committed
        consumer_offset = await consumer.committed(partition=partition)
        assert consumer_offset is None

    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
    ) as event_subscriber:
        # provide correct type information
        consumer = cast(AIOKafkaConsumer, event_subscriber._consumer)

        # check that there was no prior commit
        consumer_offset = await consumer.committed(partition=partition)
        assert consumer_offset is None

        # successfully consume one event:
        translator.consume = consume_function
        await event_subscriber.run(forever=False)

        # check that the event was committed successfully
        consumer_offset = await consumer.committed(partition=partition)
        assert consumer_offset is not None

        # publish and consume another event
        await kafka.publish_event(
            payload={"test_msg": "msg2"}, type_=type_, topic=topic
        )
        await event_subscriber.run(forever=False)
        consumer_offset += 1

        assert await consumer.committed(partition=partition) == consumer_offset

    # get a broker client to check that the commit has been propagated successfully
    client = AIOKafkaAdminClient(bootstrap_servers=kafka.kafka_servers[0])
    await client.start()

    assert topic in await client.list_topics()
    broker_offsets = await client.list_consumer_group_offsets(
        group_id=config.service_name, partitions=[partition]
    )
    assert broker_offsets[partition].offset == consumer_offset

    await client.close()


@pytest.mark.parametrize(
    "too_big",
    [True, False],
    ids=["TooBig", "AcceptableSize"],
)
async def test_publishing_with_size_limit(kafka: KafkaFixture, too_big: bool):
    """Test sending messages above or below the configured size limit"""
    # Either leave payload at ~message size or leave ample room for the rest of the event
    max_message_size = 800 * 1024  # 800 KiB
    payload = {"test_content": "a" * int(max_message_size * (1.1 if too_big else 0.9))}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    producer_config = KafkaConfig(
        service_name="test",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
        kafka_max_message_size=max_message_size,
    )

    async with KafkaEventPublisher.construct(config=producer_config) as publisher:
        with pytest.raises(MessageSizeTooLargeError) if too_big else nullcontext():
            await publisher.publish(payload=payload, type_=type_, key=key, topic=topic)


@pytest.mark.parametrize(
    "too_big",
    [True, False],
    ids=["TooBig", "AcceptableSize"],
)
async def test_consuming_with_size_limit(kafka: KafkaFixture, too_big: bool):
    """Test receiving messages above or below the configured limit"""
    max_message_size = 800 * 1024  # 800 KiB
    payload = {"test_content": "a" * int(max_message_size * (1.1 if too_big else 0.9))}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    translator = AsyncMock()
    translator.topics_of_interest = [topic]
    translator.types_of_interest = [type_]

    producer_config = KafkaConfig(
        service_name="test",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
        kafka_max_message_size=max_message_size
        * 2,  # publish messages that are too large to consume
    )

    async with KafkaEventPublisher.construct(config=producer_config) as publisher:
        await publisher.publish(payload=payload, type_=type_, key=key, topic=topic)

    consumer_config = KafkaConfig(
        service_name="test",
        service_instance_id="1",
        kafka_servers=kafka.kafka_servers,
        kafka_max_message_size=max_message_size,
    )

    async with KafkaEventSubscriber.construct(
        config=consumer_config, translator=translator
    ) as subscriber:
        await subscriber.run(forever=False)

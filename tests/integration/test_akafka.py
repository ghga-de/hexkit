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

"""Testing Apache Kafka based providers."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testutils import (  # noqa: F401
    ExpectedEvent,
    KafkaFixture,
    kafka_fixture,
)


@pytest.mark.asyncio
async def test_kafka_event_publisher(kafka_fixture: KafkaFixture):  # noqa: F811
    """Test the KafkaEventPublisher."""
    payload: JsonObject = {"test_content": "Hello World"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    config = KafkaConfig(  # type: ignore
        service_name="test_publisher",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )

    async with kafka_fixture.expect_events(
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


@pytest.mark.asyncio
async def test_kafka_event_subscriber(kafka_fixture: KafkaFixture):  # noqa: F811
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
    await kafka_fixture.publish_event(
        topic=topic, payload=payload, key=key, type_=type_
    )

    # setup the provider:
    config = KafkaConfig(  # type: ignore
        service_name="event_subscriber",
        service_instance_id="1",
        kafka_servers=kafka_fixture.kafka_servers,
    )
    async with KafkaEventSubscriber.construct(
        config=config,
        translator=translator,
    ) as event_subscriber:
        # consume one event:
        await event_subscriber.run(forever=False)

    # check if the translator was called correctly:
    translator.consume.assert_awaited_once_with(
        payload=payload, type_=type_, topic=topic
    )


def find_kafka_secrets_dir() -> Path:
    """Get the directory with Kafka secrets."""
    current_dir = Path(__file__)
    while current_dir != current_dir.parent:
        current_dir = current_dir.parent
        secrets_dir = current_dir / ".devcontainer" / "kafka_secrets"
        if secrets_dir.is_dir():
            for filename in "ca.crt", "client.crt", "client.key", "pwd.txt":
                assert (secrets_dir / filename).exists(), (
                    f"No {filename} in Kafka secrets directory."
                    " Please re-run the create_secrets.sh script."
                )
                return secrets_dir
    assert False, "Kafka secrets directory not found."


@pytest.mark.asyncio
async def test_kafka_ssl():
    """Test connecting to Kafka via SSL.

    This test uses the broker configured with the needed secrets via docker-compose
    instead of a test container.
    """
    payload: JsonObject = {"test_content": "Be aware... Connect with care"}
    type_ = "test_type"
    key = "test_key"
    topic = "test_topic"

    admin_client = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
    try:
        admin_client.delete_topics([topic])
    except KafkaError:
        pass

    secrets_dir = find_kafka_secrets_dir()
    password = open(secrets_dir / "pwd.txt").read().strip()
    assert password

    config = KafkaConfig(
        service_name="test_ssl",
        service_instance_id="1",
        kafka_servers=["localhost:19092"],  # SSL port
        security_protocol="SSL",
        ssl_cafile=str(secrets_dir / "ca.crt"),
        ssl_certfile=str(secrets_dir / "client.crt"),
        ssl_keyfile=str(secrets_dir / "client.key"),
        ssl_password=password,
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
        payload=payload, type_=type_, topic=topic
    )

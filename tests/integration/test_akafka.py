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

from os import environ
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka import (
    KafkaConfig,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.akafka.testcontainer import KafkaSSLContainer
from hexkit.providers.akafka.testutils import (  # noqa: F401
    ExpectedEvent,
    KafkaFixture,
    kafka_fixture,
)

from ..fixtures.kafka_secrets import KafkaSecrets


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


@pytest.mark.asyncio
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
            kafka_ssl_password=secrets.client_pwd,
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

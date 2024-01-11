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

"""Tests to make sure the kafka clear_topics function works for persistent fixtures."""
import pytest
from kafka import KafkaConsumer, TopicPartition

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka.testutils import KafkaFixture, get_kafka_fixture

kafka_fixture = get_kafka_fixture(scope="module")

TOPIC1 = "topic1"
TOPIC2 = "topic2"
TEST_TYPE = "test_type"
PARTITION1 = TopicPartition(TOPIC1, 0)
PARTITION2 = TopicPartition(TOPIC2, 0)


def make_payload(msg: str) -> JsonObject:
    """Return a test payload with a specified string."""
    return {"test_msg": msg}


@pytest.fixture(scope="function", autouse=True)
def reset_state(kafka_fixture: KafkaFixture):
    """Perform reset on the kafka fixture."""
    kafka_fixture.clear_topics()


@pytest.fixture(scope="module")
def persistent_consumer(kafka_fixture: KafkaFixture):
    """A module-scoped KafkaConsumer class"""
    consumer = KafkaConsumer(
        TOPIC1,
        TOPIC2,
        bootstrap_servers=kafka_fixture.kafka_servers[0],
        group_id="persistent",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=500,
    )
    yield consumer
    consumer.close()


@pytest.mark.asyncio(scope="module")
async def test_initial(kafka_fixture: KafkaFixture, persistent_consumer: KafkaConsumer):
    """Will consume some topics and leave some left over."""
    consumer = KafkaConsumer(
        TOPIC1,
        TOPIC2,
        bootstrap_servers=kafka_fixture.kafka_servers[0],
        group_id="test",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=500,
    )

    await kafka_fixture.publish_event(
        payload=make_payload("test1"), type_=TEST_TYPE, topic=TOPIC1
    )
    await kafka_fixture.publish_event(
        payload=make_payload("test2"), type_=TEST_TYPE, topic=TOPIC2
    )

    count = 0
    for _ in consumer:
        count += 1
    assert count == 2

    count = 0
    for _ in persistent_consumer:
        count += 1

        # break rather than timeout to avoid the consumer receiving next event
        if (
            persistent_consumer.position(PARTITION1) == 1
            and persistent_consumer.position(PARTITION2) == 1
        ):
            break
    assert count == 2

    await kafka_fixture.publish_event(
        payload=make_payload("test3"), type_=TEST_TYPE, topic=TOPIC2
    )

    consumer.close()


@pytest.mark.asyncio(scope="module")
async def test_remainder(
    kafka_fixture: KafkaFixture, persistent_consumer: KafkaConsumer
):
    """Will consume from same topics as first test function to verify that previous
    events are not able to be consumed by a new consumer in a new test.
    """
    consumer = KafkaConsumer(
        TOPIC1,
        TOPIC2,
        bootstrap_servers=kafka_fixture.kafka_servers[0],
        group_id="test",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=500,
    )

    # have not published any events in this function, poll to see if events exist
    leftover = 0
    for _ in consumer:
        leftover += 1
    assert leftover == 0
    consumer.close()

    # The persistent consumer should also see no events
    count = 0
    for _ in persistent_consumer:
        count += 1
    assert count == 0

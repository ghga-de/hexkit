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

"""Test the `akafka` module."""

from datetime import datetime

import pytest
from testcontainers.kafka import KafkaContainer

from hexkit.akafka import EventConsumer, EventProducer, KafkaConfigBase
from hexkit.utils import exec_with_timeout


class EventSuccessfullyConsumed(RuntimeError):
    """Raised when expected payload is received."""

    pass


def exec_dummy_func(received_payload: dict, expected_payload: dict):
    """Raises "EventSuccessfullyConsumed" exception if expected payload is received."""
    assert received_payload == expected_payload
    raise EventSuccessfullyConsumed()


def test_pub_sub():
    """Testing the Publisher and Subscriber classes."""
    topic_name = "test_topic"
    event_type = "test_event"
    event_key = "test_timestamp"
    event_payload = {"timestamp": datetime.now().isoformat()}
    event_schemas = {
        event_type: {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "additionalProperties": False,
            "properties": {
                "timestamp": {
                    "description": "The time when the message was published.",
                    "format": "date-time",
                    "type": "string",
                }
            },
            "required": ["timestamp"],
            "title": "test_schema",
            "type": "object",
        }
    }
    exec_funcs = {
        event_type: lambda received_payload: exec_dummy_func(
            received_payload, expected_payload=event_payload
        )
    }

    with KafkaContainer() as kafka:
        sub_config = KafkaConfigBase(
            service_name="test_sub",
            client_suffix="1",
            kafka_servers=[kafka.get_bootstrap_server()],
        )
        pub_config = KafkaConfigBase(
            service_name="test_pub",
            client_suffix="1",
            kafka_servers=[kafka.get_bootstrap_server()],
        )

        with EventProducer(
            config=pub_config, topic_name=topic_name, event_schemas=event_schemas
        ) as producer:
            producer.publish(
                event_type=event_type, event_key=event_key, event_payload=event_payload
            )

        with EventConsumer(
            config=sub_config,
            topic_names=[topic_name],
            exec_funcs=exec_funcs,
            event_schemas=event_schemas,
        ) as consumer:
            with pytest.raises(EventSuccessfullyConsumed):
                exec_with_timeout(
                    lambda: consumer.subscribe(run_forever=False), timeout_after=20000
                )

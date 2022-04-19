#!/usr/bin/env python3

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

"""A script for submitting example problems to the stream calculator app."""

import json
from collections import namedtuple

from kafka import KafkaConsumer, KafkaProducer
from stream_calc.config import Config

config = Config()

Event = namedtuple("Event", ["headers", "key", "value", "descr"])

events = [
    Event(
        headers=[("type", b"multiplication_problem")],
        key="example",
        value={"problem_id": "m001", "multiplier": 24, "multiplicand": 38},
        descr="24 * 38",
    ),
    Event(
        headers=[("type", b"division_problem")],
        key="example",
        value={"problem_id": "d001", "dividend": 24, "divisor": 38},
        descr="24 / 38",
    ),
]


producer = KafkaProducer(
    client_id="example_producer",
    bootstrap_servers=config.kafka_servers,
    key_serializer=lambda key: key.encode("ascii"),
    value_serializer=lambda event_value: json.dumps(event_value).encode("ascii"),
)

print("Submitted a few problems:")
for event in events:
    print(event.descr)
    producer.send(
        topic="arithmetic_problems",
        value=event.value,
        key=event.key,
        headers=event.headers,
    )

producer.flush()

print("\nAwaiting response from the stream_calc application.")

consumer = KafkaConsumer(
    "calc_output",
    client_id="example_consumer",
    group_id="example",
    bootstrap_servers=config.kafka_servers,
    auto_offset_reset="earliest",
    key_deserializer=lambda event_key: event_key.decode("ascii"),
    value_deserializer=lambda event_value: json.loads(event_value.decode("ascii")),
)

print("\nThe results are:")
for submitted_event in events:
    received_event = next(consumer)
    assert (  # nosec
        submitted_event.value["problem_id"] == received_event.value["problem_id"]
    )
    assert received_event.headers[0][0] == "type"  # nosec
    received_type = received_event.headers[0][1].decode("ascii")
    assert received_type == "calc_success"  # nosec
    print(received_event.value["result"])

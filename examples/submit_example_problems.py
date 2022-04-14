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

from kafka import KafkaProducer
from stream_calc.config import Config

config = Config()

Event = namedtuple("Event", ["headers", "key", "value"])

events = [
    Event(
        headers=[("type", b"multiplication_problem")],
        key="example",
        value={"problem_id": "m001", "multiplier": 24, "multiplicand": 38},
    ),
    Event(
        headers=[("type", b"devision_problem")],
        key="example",
        value={"problem_id": "d001", "dividend": 24, "divisor": 38},
    ),
]


producer = KafkaProducer(
    client_id="example_producer",
    bootstrap_servers=config.kafka_servers,
    key_serializer=lambda key: key.encode("ascii"),
    value_serializer=lambda event_value: json.dumps(event_value).encode("ascii"),
)

for event in events:
    producer.send(
        topic="arithmetic_problems",
        value=event.value,
        key=event.key,
        headers=event.headers,
    )

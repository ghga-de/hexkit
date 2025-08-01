# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""
Test all components in integration when addressing the app via its event-driven API.

Please note, these tests are written in a way so that they can be reused by the
`../../submit_example_problems.py` demo script.
"""

import json
from typing import NamedTuple

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from testcontainers.kafka import KafkaContainer

from hexkit.custom_types import JsonObject
from hexkit.providers.akafka.testcontainer import DEFAULT_IMAGE as KAFKA_IMAGE
from stream_calc.config import Config
from stream_calc.main import main

DEFAULT_CONFIG = Config()
CORRELATION_ID_HEADER = (
    "correlation_id",
    bytes("7041eb31-7333-4b57-97d7-90f5562c3383", encoding="ascii"),
)


class Event(NamedTuple):
    """
    Describes the content of an event.
    (The topic and key are considered delivery details and are thus not included.)

    Args:
        type_: The event type that identifies the payload schema.
        payload: The actual payload of the event.
    """

    type_: str
    payload: JsonObject


class Case(NamedTuple):
    """
    Describes a test case.

    Args:
        description: Human readable description.
        problem: The event containing the arithmetic problem that shall be submitted.
        outcome: The outcome/resulting event expected as a response.
    """

    description: str
    problem: Event
    outcome: Event


CASES = [
    Case(
        description="24 * 38",
        problem=Event(
            type_="multiplication_problem",
            payload={"problem_id": "m001", "multiplier": 24, "multiplicand": 38},
        ),
        outcome=Event(
            type_=DEFAULT_CONFIG.result_emit_success_type,
            payload={"problem_id": "m001", "result": 912},
        ),
    ),
    Case(
        description="24 / 38",
        problem=Event(
            type_="division_problem",
            payload={"problem_id": "d001", "dividend": 24, "divisor": 38},
        ),
        outcome=Event(
            type_=DEFAULT_CONFIG.result_emit_success_type,
            payload={"problem_id": "d001", "result": 0.631578947368421},
        ),
    ),
    Case(
        description="1 / 0",
        problem=Event(
            type_="division_problem",
            payload={"problem_id": "d002", "dividend": 1, "divisor": 0},
        ),
        outcome=Event(
            type_=DEFAULT_CONFIG.result_emit_failure_type,
            payload={"problem_id": "d002", "reason": "The divisor may not be zero."},
        ),
    ),
]


async def submit_test_problems(
    cases: list[Case],
    *,
    kafka_server: str,
    topic: str = DEFAULT_CONFIG.problem_receive_topics[0],
) -> None:
    """Will submit a set of arithmetic problems as events to the stream calc
    application.
    (To be also used by the demo script. --> Reason for the print statements.)

    Args:
        cases:
            List of Cases containing the problems to submit.
        kafka_server:
            The path to the kafka server to publish to.
        topic:
            The topic to submit the problem events to.
    """

    producer = AIOKafkaProducer(
        client_id="test_producer",
        bootstrap_servers=[kafka_server],
        key_serializer=lambda key: key.encode("ascii"),
        value_serializer=lambda event_value: json.dumps(event_value).encode("ascii"),
    )
    await producer.start()

    print("Submitted a few problems:")
    for case in cases:
        print(case.description)
        await producer.send(
            topic=topic,
            value=case.problem.payload,
            key="test_examples",
            headers=[
                ("type", case.problem.type_.encode("ascii")),
                CORRELATION_ID_HEADER,
            ],
        )

    await producer.flush()
    await producer.stop()


async def check_problem_outcomes(
    cases: list[Case],
    *,
    kafka_server: str,
    topic: str = DEFAULT_CONFIG.result_emit_output_topic,
) -> None:
    """Will await the outcome of problems previously submitted.
    (To be also used by the demo script. --> Reason for the print statements.)

    Args:
        cases:
            List of Cases containing the problems that where submitted and their
            expected outcome.
        kafka_server:
            The path to the kafka server to publish to.
        topic:
            The topic to expect the outcome events to arrive in.
    """

    consumer = AIOKafkaConsumer(
        topic,
        client_id="example_consumer",
        group_id="example",
        bootstrap_servers=[kafka_server],
        auto_offset_reset="earliest",
        key_deserializer=lambda event_key: event_key.decode("ascii"),
        value_deserializer=lambda event_value: json.loads(event_value.decode("ascii")),
    )
    await consumer.start()

    cases = cases[::-1]

    async for received_record in consumer:
        case = cases.pop()

        # check if received event contains the expected content:
        assert received_record.headers[0][0] == "type"
        type_ = received_record.headers[0][1].decode("ascii")
        received_event = Event(type_=type_, payload=received_record.value)
        assert received_event == case.outcome

        # print out the outcome:
        if type_ == "calc_success":
            print(received_event.payload["result"])
        elif type_ == "calc_failure":
            reason = received_event.payload["reason"]
            problem_id = received_event.payload["problem_id"]
            print(f"The problem with ID {problem_id} failed: {reason}")
        else:
            raise ValueError(f"Unkown event type: {type_}")

        if not cases:
            break

    assert not cases

    await consumer.stop()


@pytest.mark.asyncio
async def test_receive_calc_publish():
    """
    Test the receipt of new arithmetic problems, the calculation, and the publication of
    the results.
    """

    with KafkaContainer(image=KAFKA_IMAGE) as kafka:
        kafka_server = kafka.get_bootstrap_server()

        await submit_test_problems(CASES, kafka_server=kafka_server)

        # run the stream_calc app:
        # (for each problem separately to avoid running forever)
        config = Config(kafka_servers=[kafka_server])
        for _ in CASES:
            await main(config=config, run_forever=False)

        await check_problem_outcomes(CASES, kafka_server=kafka_server)

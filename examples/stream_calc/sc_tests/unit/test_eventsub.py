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

"""Testing the `translators.eventsub` module."""

from unittest.mock import AsyncMock

import pytest

from hexkit.custom_types import JsonObject
from stream_calc.translators.eventsub import (
    EventProblemReceiver,
    EventProblemReceiverConfig,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload, type_, handler_method",
    [
        (
            {"problem_id": "some_problem", "multiplier": 2.5, "multiplicand": 10},
            "multiplication_problem",
            "multiply",
        ),
        (
            {"problem_id": "another_problem", "dividend": 25, "divisor": 10},
            "division_problem",
            "divide",
        ),
    ],
)
async def test_event_problem_receiver(
    payload: JsonObject, type_: str, handler_method: str
):
    """Test the EventProblemReceiver to consume events."""
    topic = "test_topic"

    problem_handler = AsyncMock()

    problem_receiver = EventProblemReceiver(
        config=EventProblemReceiverConfig(), problem_handler=problem_handler
    )
    await problem_receiver.consume(
        payload=payload, type_=type_, topic=topic, key="test"
    )

    # check the published event:
    mock_method = getattr(problem_handler, handler_method)
    mock_method.assert_awaited_once()


@pytest.mark.asyncio
async def test_event_problem_receiver_with_missing_field():
    """Test the EventProblemReceiver to report missing fields."""
    topic = "test_topic"
    type_ = "multiplication_problem"
    payload: JsonObject = {"problem_id": "bad_problem", "multiplier": 0}
    problem_handler = AsyncMock()
    problem_receiver = EventProblemReceiver(
        config=EventProblemReceiverConfig(), problem_handler=problem_handler
    )
    with pytest.raises(
        EventProblemReceiver.MalformedPayloadError,
        match="did not contain the expected field 'multiplicand'",
    ):
        await problem_receiver.consume(
            payload=payload, type_=type_, topic=topic, key="test"
        )
    problem_handler.multiply.assert_not_awaited()

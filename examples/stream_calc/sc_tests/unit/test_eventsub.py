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

"""Testing the `translators.eventsub` module."""

from unittest.mock import AsyncMock

import pytest
from stream_calc.translators.eventsub import EventProblemReceiver

from hexkit.custom_types import JsonObject


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
        topics_of_interest=[topic], problem_handler=problem_handler
    )
    await problem_receiver.consume(payload=payload, type_=type_, topic=topic)

    # check the published event:
    mock_method = getattr(problem_handler, handler_method)
    mock_method.assert_awaited_once()

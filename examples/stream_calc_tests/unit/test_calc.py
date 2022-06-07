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

"""Testing the `core.calc` module."""

from typing import Optional
from unittest.mock import AsyncMock

import pytest
from stream_calc.core.calc import StreamCalculator


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "multiplier, multiplicand, expected_result", [(2.5, 10.0, 25), (-1, 1, -1)]
)
async def test_stream_calculator_multiply(
    multiplier: float,
    multiplicand: float,
    expected_result: Optional[float],
):
    """Test the `multiply` method of the StreamCalculator"""
    problem_id = "some_problem"
    result_emitter = AsyncMock()

    stream_calc = StreamCalculator(result_emitter=result_emitter)
    await stream_calc.multiply(
        problem_id=problem_id, multiplier=multiplier, multiplicand=multiplicand
    )

    # check if the result emitter was used correctly and with the expected result:
    result_emitter.emit_result.assert_awaited_once_with(
        problem_id=problem_id, result=expected_result
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dividend, divisor, expected_result",
    [(25.0, 10.0, 2.5), (1, 0, None)],
)
async def test_stream_calculator_devide(
    dividend: float,
    divisor: float,
    expected_result: Optional[float],
):
    """Test the `divide` method of the StreamCalculator.
    `expected_result` is `None` when a failure is expected.

    """
    problem_id = "some_problem"
    result_emitter = AsyncMock()

    stream_calc = StreamCalculator(result_emitter=result_emitter)
    await stream_calc.divide(problem_id=problem_id, dividend=dividend, divisor=divisor)

    # check if the result emitter was used correctly:
    if expected_result:
        # the calculation was successful, check the expected result:
        result_emitter.emit_result.assert_awaited_once_with(
            problem_id=problem_id, result=expected_result
        )
    else:
        # the calculation failed:
        result_emitter.emit_failure.assert_awaited_once()
        assert result_emitter.emit_failure.await_args.kwargs["problem_id"] == problem_id

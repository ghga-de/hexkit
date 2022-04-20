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

"""The application logic of the calculator."""

from examples.stream_calc.ports.problem_receiver import ArithProblemReceiverPort
from examples.stream_calc.ports.result_emitter import CalcResultEmitterPort


class StreamCalculator(ArithProblemReceiverPort):
    """
    Perform calculations and stream results.

    More operations like addition or subtraction could be added in a similar way.
    """

    def __init__(self, result_emitter: CalcResultEmitterPort):
        """Configure relevant ports."""

        self._result_emitter = result_emitter

    def multiply(self, problem_id: str, multiplier: float, multiplicand: float):
        """Multiply the multiplicand with the multiplier."""

        result = multiplier * multiplicand
        self._result_emitter.emit_result(problem_id=problem_id, result=result)

    def divide(self, problem_id: str, dividend: float, divisor: float):
        """Divide the dividend by the divisor."""

        try:
            result = dividend / divisor
            self._result_emitter.emit_result(problem_id=problem_id, result=result)
        except ZeroDivisionError:
            self._result_emitter.emit_failure(
                problem_id=problem_id, reason="The divisor may not be zero."
            )

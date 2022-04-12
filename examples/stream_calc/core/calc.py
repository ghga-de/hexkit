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

from stream_calc.ports.outbound.result_emitter import ResultEmitterPort

from examples.stream_calc.ports.inbound.task_receiver import TaskReceiverPort


class StreamCalculator(TaskReceiverPort):
    """
    Perform calculations and stream results.

    More operations like addition or subtraction could be added in a similar way.
    """

    def __init__(self, result_emitter: ResultEmitterPort):
        """Configure relevant ports."""

        self._result_emitter = result_emitter

    def multiply(self, task_id: str, multiplier: float, multiplicand: float):
        """Multiply the multiplicand with the multiplier."""

        result = multiplier * multiplicand
        self._result_emitter.emit_result(task_id=task_id, result=result)

    def devide(self, task_id: str, dividend: float, divisor: float):
        """Dive the dividend by the divisor."""

        try:
            result = dividend / divisor
            self._result_emitter.emit_result(task_id=task_id, result=result)
        except ZeroDivisionError:
            self._result_emitter.emit_failure(
                task_id=task_id, reason="The divisor may not be zero."
            )

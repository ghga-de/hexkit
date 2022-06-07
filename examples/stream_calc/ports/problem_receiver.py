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

"""Inbound port for communicating results."""

from abc import ABC, abstractmethod


class ArithProblemHandlerPort(ABC):
    """
    Port that excepts calculation tasks.
    More operations like addition or subtraction could be added in a similar way.
    """

    @abstractmethod
    async def multiply(self, problem_id: str, multiplier: float, multiplicand: float):
        """Multiply the multiplicand with the multiplier."""
        ...

    @abstractmethod
    async def divide(self, problem_id: str, dividend: float, divisor: float):
        """Divide the dividend by the divisor."""
        ...

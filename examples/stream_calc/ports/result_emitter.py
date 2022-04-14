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

"""Outbound port for communicating results."""

from typing import Protocol


class CalcResultEmitterPort(Protocol):
    """Emits results of a calculation."""

    def emit_result(self, *, task_id: str, result: float) -> None:
        """Emits the result of the calc task with the given ID."""
        ...

    def emit_failure(self, *, task_id: str, reason: str) -> None:
        """Emits message that the calc task with the given ID could not be solved."""
        ...

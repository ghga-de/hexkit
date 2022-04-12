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

"""Translators that target the event publishing protocol."""

from examples.stream_calc.ports.result_emitter import CalcResultEmitterPort
from hexkit.custom_types import JsonObject
from hexkit.eventpub.protocol import EventPublisherProtocol


class EventResultEmitter(CalcResultEmitterPort):
    """Abstract translator translating between the ResultEmitterPort and the
    EventPubProtocol."""

    def __init__(self, event_publisher: EventPublisherProtocol) -> None:
        """Configure with provider for the the EventPublisherProto"""

        self._event_publisher = event_publisher

    def emit_result(self, *, task_id: str, result: float) -> None:
        """Emits the result of the calc task with the given ID."""

        payload: JsonObject = {"task_id": task_id, "result": result}

        self._event_publisher.publish(
            payload=payload,
            type_="calc_success",
            key_=task_id,
            topic="calc_output",
        )

    def emit_failure(self, *, task_id: str, reason: str) -> None:
        """Emits message that the calc task with the given ID could not be solved."""

        payload: JsonObject = {"task_id": task_id, "reason": reason}

        self._event_publisher.publish(
            payload=payload,
            type_="calc_failure",
            key_=task_id,
            topic="calc_output",
        )

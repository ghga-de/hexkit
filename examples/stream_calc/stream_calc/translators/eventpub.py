# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

from pydantic_settings import BaseSettings

from hexkit.custom_types import JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol
from stream_calc.ports.result_emitter import CalcResultEmitterPort


class EventResultEmitterConfig(BaseSettings):
    """Config parameters and their defaults."""

    result_emit_output_topic: str = "calc_output"
    result_emit_success_type: str = "calc_success"
    result_emit_failure_type: str = "calc_failure"


class EventResultEmitter(CalcResultEmitterPort):
    """Outbound abstract translator translating between the ResultEmitterPort and the
    EventPubProtocol."""

    def __init__(
        self,
        *,
        config: EventResultEmitterConfig,
        event_publisher: EventPublisherProtocol,
    ) -> None:
        """Configure with provider for the the EventPublisherProto"""

        self._event_publisher = event_publisher
        self._output_topic = config.result_emit_output_topic
        self._success_type = config.result_emit_success_type
        self._failure_type = config.result_emit_failure_type

    async def emit_result(self, *, problem_id: str, result: float) -> None:
        """Emits the result of the calc task with the given ID."""

        payload: JsonObject = {"problem_id": problem_id, "result": result}

        await self._event_publisher.publish(
            payload=payload,
            type_=self._success_type,
            key=problem_id,
            topic=self._output_topic,
        )

    async def emit_failure(self, *, problem_id: str, reason: str) -> None:
        """Emits message that the calc task with the given ID could not be solved."""

        payload: JsonObject = {"problem_id": problem_id, "reason": reason}

        await self._event_publisher.publish(
            payload=payload,
            type_=self._failure_type,
            key=problem_id,
            topic=self._output_topic,
        )

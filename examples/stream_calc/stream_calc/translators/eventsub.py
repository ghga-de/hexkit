# Copyright 2021 - 2023 Universität Tübingen, DKFZ and EMBL
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

from pydantic import BaseSettings
from stream_calc.ports.problem_receiver import ArithProblemHandlerPort

from hexkit.custom_types import Ascii, JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol


class EventProblemReceiverConfig(BaseSettings):
    """Config parameters and their defaults."""

    problem_receive_topics: list[str] = ["arithmetic_problems"]


class EventProblemReceiver(EventSubscriberProtocol):
    """Inbound abstract translator translating between the EventSubscriberProtocol and the
    ArithProblemReceiverPort."""

    types_of_interest = ["multiplication_problem", "division_problem"]

    def __init__(
        self,
        *,
        config: EventProblemReceiverConfig,
        problem_handler: ArithProblemHandlerPort,
    ):
        """Configure the translator with a corresponding port implementation."""
        self._problem_handler = problem_handler
        self.topics_of_interest = config.problem_receive_topics

    @classmethod
    def _extract_payload(cls, payload: JsonObject, expected_fields: list[str]) -> tuple:
        """
        Checks whether all the expected fields are present in the payload and
        returns their values as a tuple.
        """
        values = []
        for field in expected_fields:
            if field not in payload:
                raise cls.MalformedPayloadError(
                    f"Payload did not contain the expected field '{field}'"
                )
            values.append(payload[field])

        return tuple(values)

    async def _consume_validated(
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        # This implementation does NOT use the `topic` information that is provided as
        # part of the EventSubscriberProtocol:
        topic: Ascii,  # pylint: disable=unused-argument
    ) -> None:
        """
        Receive and process an event with already validated topic and type.

        Args:
            payload: The data/payload to send with the event.
            type_: The type of the event.
            topic: Name of the topic the event was published to.
        """

        if type_ == "multiplication_problem":
            problem_id, multiplier, multiplicand = self._extract_payload(
                payload, expected_fields=["problem_id", "multiplier", "multiplicand"]
            )
            await self._problem_handler.multiply(
                problem_id=problem_id,
                multiplier=multiplier,
                multiplicand=multiplicand,
            )

        elif type_ == "division_problem":
            problem_id, dividend, divisor = self._extract_payload(
                payload, expected_fields=["problem_id", "dividend", "divisor"]
            )
            await self._problem_handler.divide(
                problem_id=problem_id,
                dividend=dividend,
                divisor=divisor,
            )

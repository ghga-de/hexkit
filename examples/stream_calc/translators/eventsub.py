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

from examples.stream_calc.ports.problem_receiver import ArithProblemReceiverPort
from hexkit.custom_types import JsonObject
from hexkit.protocols.eventsub import EventSubscriberProtocol


class EventProblemReceiver(EventSubscriberProtocol):
    """Inbound abstract translator translating between the EventSubscriberProtocol and the
    ArithProblemReceiverPort."""

    topics_of_interest = ["arithmetic_problems"]
    types_of_interest = ["multiplication_problem", "division_problem"]

    def __init__(self, problem_receiver: ArithProblemReceiverPort):
        """Configure the translator with a corresponding port implementation."""
        super().__init__()
        self._problem_receiver = problem_receiver

    def _check_payload(self, payload: JsonObject, expected_fields: list[str]):
        """Check whether all the expected fields are present in the payload."""
        for field in expected_fields:
            if field not in payload:
                raise self.MalformedPayloadError(
                    f"Payload did not contain the expected field '{field}'"
                )

    async def consume(self, *, payload: JsonObject, type_: str, topic: str) -> None:
        """Receive an event of interest and process it according to its type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str):
                Name of the topic to publish the event to.
                Not used by this implementation.
        """

        await super().consume(payload=payload, type_=type_, topic=topic)

        if type_ == "multiplication_problem":
            self._check_payload(
                payload, expected_fields=["problem_id", "multiplier", "multiplicand"]
            )
            await self._problem_receiver.multiply(
                problem_id=payload["problem_id"],
                multiplier=payload["multiplier"],
                multiplicand=payload["multiplicand"],
            )

        elif type_ == "division_problem":
            self._check_payload(
                payload, expected_fields=["problem_id", "dividend", "divisor"]
            )
            await self._problem_receiver.divide(
                problem_id=payload["problem_id"],
                dividend=payload["dividend"],
                divisor=payload["divisor"],
            )

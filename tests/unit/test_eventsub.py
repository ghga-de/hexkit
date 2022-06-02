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

"""Testing the event publishing protocol."""

from contextlib import nullcontext

import pytest

from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.utils import NonAsciiStrError


class FakeSubscriber(EventSubscriberProtocol):
    """
    Implements the EventSubscriberProtocol abstract class without providing
    any logic.
    """

    async def consume(self, *, payload, type_, topic) -> None:
        await super().consume(payload=payload, type_=type_, topic=topic)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "type_, topic, exception",
    [
        ("test_type", "test_topic", None),
        (
            "test_ßtype",  # non ascii
            "test_topic",
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_ßtopic",  # non ascii
            NonAsciiStrError,
        ),
    ],
)
async def test_ascii_val(type_, topic, exception):
    """Tests the ASCII validation logic included in the EventSubscriberProtocol."""
    payload = {"test_content": "Hello World"}

    # create event publisher:
    event_submitter = FakeSubscriber()

    # publish event using the provider:
    with (pytest.raises(exception) if exception else nullcontext()):  # type: ignore
        await event_submitter.consume(
            payload=payload,
            type_=type_,
            topic=topic,
        )

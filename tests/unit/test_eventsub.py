# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
from typing import Optional
from uuid import UUID

import pytest

from hexkit.protocols.eventsub import EventSubscriberProtocol
from hexkit.utils import NonAsciiStrError


class FakeSubscriber(EventSubscriberProtocol):
    """
    Implements the EventSubscriberProtocol abstract class without providing
    any logic.
    """

    async def _consume_validated(self, *, payload, type_, topic, key, event_id) -> None:
        pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "type_, topic, key, exception",
    [
        ("test_type", "test_topic", "test_key", None),
        (
            "test_ßtype",  # non ascii
            "test_topic",
            "test_key",
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_ßtopic",  # non ascii
            "test_key",
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_topic",
            "test_ßkey",  # non ascii
            NonAsciiStrError,
        ),
    ],
)
async def test_ascii_val(
    type_: str, topic: str, key: str, exception: Optional[type[Exception]]
):
    """Tests the ASCII validation logic included in the EventSubscriberProtocol."""
    payload = {"test_content": "Hello World"}

    # create event publisher:
    event_submitter = FakeSubscriber()
    test_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")

    # publish event using the provider:
    with pytest.raises(exception) if exception else nullcontext():
        await event_submitter.consume(
            payload=payload, type_=type_, topic=topic, key=key, event_id=test_uuid
        )

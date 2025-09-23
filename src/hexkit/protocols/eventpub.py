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

"""Protocol related to event publishing."""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from uuid import UUID, uuid4

from pydantic import UUID4

from hexkit.custom_types import Ascii, JsonObject
from hexkit.utils import check_ascii


class EventPublisherProtocol(ABC):
    """A protocol for publishing events to an event broker."""

    async def publish(  # noqa: PLR0913
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        key: Ascii,
        topic: Ascii,
        event_id: UUID4 | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish an event.

        Args:
        - `payload` (JSON): The payload to ship with the event.
        - `type_` (str): The event type. ASCII characters only.
        - `key` (str): The event type. ASCII characters only.
        - `topic` (str): The event type. ASCII characters only.
        - `event_id` (UUID4, optional): An optional event ID. If not provided, a new
          one will be generated.
        - `headers`: Additional headers to attach to the event.
        """
        if event_id:
            if not isinstance(event_id, UUID):
                raise TypeError(f"event_id must be a UUID, got {type(event_id)}")
        else:
            event_id = uuid4()

        check_ascii(type_, key, topic)
        if headers is None:
            headers = {}

        await self._publish_validated(
            payload=payload,
            type_=type_,
            key=key,
            topic=topic,
            event_id=event_id,
            headers=headers,
        )

    @abstractmethod
    async def _publish_validated(  # noqa: PLR0913
        self,
        *,
        payload: JsonObject,
        type_: Ascii,
        key: Ascii,
        topic: Ascii,
        event_id: UUID4,
        headers: Mapping[str, str],
    ) -> None:
        """Publish an event with already validated topic and type.

        Args:
        - `payload` (JSON): The payload to ship with the event.
        - `type_` (str): The event type. ASCII characters only.
        - `key` (str): The event type. ASCII characters only.
        - `topic` (str): The event type. ASCII characters only.
        - `event_id` (UUID): The event ID.
        - `headers`: Additional headers to attach to the event.
        """
        ...

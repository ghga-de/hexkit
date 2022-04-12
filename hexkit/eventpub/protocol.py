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

"""Protocols related to event handling."""

from typing import Protocol

from hexkit.custom_types import JsonObject


class EventPublisherProtocol(Protocol):
    """A protocol for publishing events to an event broker."""

    def publish(
        self, *, payload: JsonObject, type_: str, key_: str, topic: str
    ) -> None:
        """Publish an event.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key_ (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        ...

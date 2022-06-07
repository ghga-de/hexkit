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

"""
An in-memory provider for event publishing.

ATTENTION: For testing purposes only.
"""

from collections import defaultdict, deque
from typing import NamedTuple, Optional

from hexkit.custom_types import JsonObject
from hexkit.protocols.eventpub import EventPublisherProtocol


class TopicExhaustedError(RuntimeError):
    """Thrown when now more event are queued in a topic."""


class Event(NamedTuple):
    """Container for specifying event information."""

    type_: str
    key: str
    payload: JsonObject


class InMemEventStore:
    """
    A manager for multiple topics, whereby each topic is model as queue not as log.

    It should be seen as a utility of the `InMemEventPublisher` and should only together
    with that class.
    """

    def __init__(self):
        """Create an in memory topic registry based on collections' deque."""
        self.topics: dict[str, deque[Event]] = defaultdict(deque)

    def post(self, topic: str, event: Event) -> None:
        """Queue a new event to a topic."""

        self.topics[topic].append(event)

    def get(self, topic) -> Event:
        """Get the next element in the queue corresponding to the specified topic."""

        try:
            return self.topics[topic].popleft()
        except IndexError as error:
            raise TopicExhaustedError() from error


class InMemEventPublisher(EventPublisherProtocol):
    """
    An in-memory EventPublisher for testing purposes.
    Please note, this only works when publisher and consumers are running in the same
    thread. Not suitable for inter-thread or inter-process comminication.
    """

    def __init__(self, event_store: Optional[InMemEventStore] = None):
        """
        Initialize with existing event_store or let it create a new one.
        """
        self.event_store = event_store if event_store else InMemEventStore()

    async def _publish_validated(
        self, *, payload: JsonObject, type_: str, key: str, topic: str
    ) -> None:
        """Publish an event with already validated topic and type.

        Args:
            payload (JSON): The payload to ship with the event.
            type_ (str): The event type. ASCII characters only.
            key (str): The event type. ASCII characters only.
            topic (str): The event type. ASCII characters only.
        """
        event = Event(type_=type_, key=key, payload=payload)
        self.event_store.post(topic=topic, event=event)

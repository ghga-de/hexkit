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

"""Protocol related to event subscription."""

from abc import ABC, abstractmethod

from hexkit.custom_types import Ascii, JsonObject
from hexkit.utils import check_ascii


class EventSubscriberProtocol(ABC):
    """
    A protocol for consuming events to an event broker.

    In addition to the methods described below, implementations shall expose the
    the following attributes:
            topics_of_interest (list[str]):
                A list of topics of interest to which the provider shall subscribe.
            types_of_interest (list[str]):
                A list of event types of interest. Out of the events arriving in the
                topics of interest, the provider shall only pass events of these types
                down to the protocol. Events of other types shall be filtered out.
    """

    class MalformedPayloadError(RuntimeError):
        """
        Raised if the payload of a received event was not formatted as expected given
        the type.
        """

    topics_of_interest: list[Ascii]
    types_of_interest: list[Ascii]

    @abstractmethod
    def consume(self, *, payload: JsonObject, type_: Ascii, topic: Ascii) -> None:
        """Receive an event of interest and process it according to its type.

        Args:
            payload (JsonObject): The data/payload to send with the event.
            type_ (str): The type of the event.
            topic (str): Name of the topic to publish the event to.
        """
        check_ascii([type_, topic])
        ...

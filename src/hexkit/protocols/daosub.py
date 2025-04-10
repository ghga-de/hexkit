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

"""A protocol for consuming events published through the DaoPublisherFactoryProtocol."""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

Dto = TypeVar("Dto", bound=BaseModel)


class DtoValidationError(ValueError):
    """Raised when the payload of a received event was not formatted as expected."""


class DaoSubscriberProtocol(ABC, Generic[Dto]):
    """A protocol for consuming events published through the DaoPublisherFactoryProtocol.

    In addition to the methods described below, implementations shall expose the
    the following attributes:
            event_topic:
                The name of the topic from which updates for the given resource are
                consumed.
            dto_model:
                A pydantic model representing the data transfer object (DTO) for the
                payload of changed events.

    If the upstream provider using this protocol fails to convert the payload of a
    change event to the expected DTO model, it should raise a DtoValidationError.
    """

    event_topic: str
    dto_model: type[Dto]

    @abstractmethod
    async def changed(self, resource_id: str, update: Dto) -> None:
        """Consume a change event (created or updated) for the resource with the given
        ID.
        """
        ...

    @abstractmethod
    async def deleted(self, resource_id: str) -> None:
        """Consume an event indicating the deletion of the resource with the given ID."""
        ...

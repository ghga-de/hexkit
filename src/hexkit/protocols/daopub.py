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

"""Protocol for creating Data Access Objects to perform CRUD (plus find) interactions
with the database plus automatically publish the changes as events using a modification
of the outbox pattern.
"""

# ruff: noqa: PLR0913

import typing
from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Callable, Optional

from hexkit.custom_types import JsonObject
from hexkit.protocols.dao import (
    Dao,
    DaoFactoryBase,
    Dto,
)


class DaoPublisher(Dao[Dto], typing.Protocol[Dto]):
    """A Data Access Object (DAO) that automatically publishes changes according to the
    outbox pattern.
    """

    async def publish_pending(self) -> None:
        """Publishes all non-published changes."""
        ...

    async def republish(self) -> None:
        """Republishes the state of all resources independent of whether they have
        already been published or not.
        """
        ...


class DaoPublisherFactoryProtocol(DaoFactoryBase, ABC):
    """A protocol describing a factory to produce Data Access Objects (DAO) objects
    which automatically publish changes according to the outbox pattern.
    """

    async def get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]] = None,
        dto_to_event: Callable[[Dto], Optional[JsonObject]],
        event_topic: str,
        autopublish: bool = True,
    ) -> DaoPublisher[Dto]:
        """Constructs an Outbox DAO for interacting with resources in a database.

        Args:
            name:
                The name of the resource type (roughly equivalent to the name of a
                database table or collection).
            dto_model:
                A DTO (Data Transfer Object) model describing the shape of resources.
            id_field:
                The name of the field of the `dto_model` that serves as resource ID.
                (DAO implementation might use this field as primary key.)
            fields_to_index:
                Optionally, provide any fields that should be indexed in addition to the
                `id_field`. Defaults to None.
            dto_to_event:
                A function that takes a DTO and returns the payload for an event.
                If the returned payload is None, the event will not be published.
            event_topic:
                The topic to which events should be published.
            autopublish:
                Whether to automatically publish changes. Defaults to True.

        Returns:
            A DAO of type DaoPublisher, which requires ID specification upon resource
            creation.

        Raises:
            self.IdFieldNotFoundError:
                Raised when the dto_model did not contain the expected id_field.
            self.IdTypeNotSupportedError:
                Raised when the id_field of the dto_model has an unexpected type.
        """
        self._validate(
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
        )

        return await self._get_dao(
            name=name,
            dto_model=dto_model,
            id_field=id_field,
            fields_to_index=fields_to_index,
            dto_to_event=dto_to_event,
            event_topic=event_topic,
            autopublish=autopublish,
        )

    @abstractmethod
    async def _get_dao(
        self,
        *,
        name: str,
        dto_model: type[Dto],
        id_field: str,
        fields_to_index: Optional[Collection[str]],
        dto_to_event: Callable[[Dto], Optional[JsonObject]],
        event_topic: str,
        autopublish: bool,
    ) -> DaoPublisher[Dto]:
        """*To be implemented by the provider. Input validation is done outside of this
        method.*
        """
        ...

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

"""Prefab functions to help with migrations caused by changes in Hexkit."""

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from uuid import UUID, uuid4

from hexkit.providers.mongodb.migrations import Document
from hexkit.utils import round_datetime_to_ms

__all__ = [
    "convert_outbox_correlation_id_v6",
    "convert_persistent_event_v6",
    "convert_uuids_and_datetimes_v6",
]


def convert_uuids_and_datetimes_v6(
    *,
    uuid_fields: list[str] | None = None,
    date_fields: list[str] | None = None,
) -> Callable[[Document], Awaitable[Document]]:
    """Produce a function to convert a document to the format expected by Hexkit v6.

    If `uuid_fields` is provided, it will convert those fields from string to UUID.
    If `date_fields` is provided, it will convert those fields from isoformat strings
    to UTC datetime objects with the microseconds rounded to milliseconds.

    Args:
        uuid_fields: List of fields currently storing UUIDs in string format.
        date_fields: List of fields currently storing datetimes as isoformat strings.
    """

    async def convert(doc: Document) -> Document:
        """Convert the document."""
        for field in uuid_fields or []:
            doc[field] = UUID(doc[field])
        for field in date_fields or []:
            old_dt = datetime.fromisoformat(doc[field])
            if old_dt.tzname() != "UTC":
                old_dt = old_dt.astimezone(timezone.utc)
            doc[field] = round_datetime_to_ms(old_dt)
        return doc

    return convert


async def convert_outbox_correlation_id_v6(doc: Document) -> Document:
    """Convert an outbox document's correlation ID to the format expected by Hexkit v6.

    This will only convert the __metadata__.correlation_id to a UUID.

    If you need to modify other fields, you must define that logic separately.
    """
    if correlation_id_str := doc.get("__metadata__", {}).get("correlation_id", ""):
        doc["__metadata__"]["correlation_id"] = UUID(correlation_id_str)
    return doc


_convert_persistent_event_existing_fields_v6 = convert_uuids_and_datetimes_v6(
    uuid_fields=["correlation_id"], date_fields=["created"]
)


async def convert_persistent_event_v6(doc: Document) -> Document:
    """Convert a persistent event document to the format expected by Hexkit v6.

    This will:
    - convert correlation_id to UUID
    - convert created to UTC datetime with milliseconds precision
    - populate event_id with UUID

    If you need to modify other fields, you must define that logic separately.
    """
    doc = await _convert_persistent_event_existing_fields_v6(doc)
    try:
        # Attempt to convert the _id field to a UUID, since it is either a random
        # UUID string or a non-UUID string in form "topic:key".
        doc["event_id"] = UUID(doc["_id"])
    except ValueError:
        doc["event_id"] = uuid4()  # Fallback to a new UUID if conversion fails
    return doc

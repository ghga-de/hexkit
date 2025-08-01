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

"""General utilities that don't require heavy dependencies."""

from collections.abc import Collection
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel

LOWER_BOUND = 8 * 1024**2
UPPER_BOUND = 4 * 1024**3
FILE_SIZE_LIMIT = 5 * 1024**4
MAX_NUM_PARTS = 10_000


class FieldNotInModelError(RuntimeError):
    """Raised when provided fields where not contained in a pydantic model."""

    def __init__(self, *, model: type[BaseModel], unexpected_field: Collection[str]):
        message = (
            f"The pydantic model {model} does not contain following fields: "
            + str(unexpected_field)
        )
        super().__init__(message)


class NonAsciiStrError(RuntimeError):
    """Thrown when non-ASCII string was unexpectedly provided"""

    def __init__(self, str_value: str):
        """Prepare custom message."""
        super().__init__(f"Non-ASCII string provided: {str_value!r}")


def calc_part_size(*, file_size: int, preferred_part_size: Optional[int] = None) -> int:
    """
    Gives recommendations on the part_size to use for up- or download of a file given
    it's total size. It makes sure that chosen part size is within bounds and produces
    <= 10_000 parts.
    Args:
        file_size (int): total file size in bytes
        preferred_part_size (int):
            You may provide your preferred part size in bytes. If it satisfies the technical
            constraints, it will be returned unchanged. Otherwise, it is ignored and a
            technically viable part size is chosen instead.

    Returns: a recommendation for the part size in bytes.

    Raises:
        ValueError if file size exceeds the maximum of 5 TiB
    """
    if preferred_part_size is None:
        preferred_part_size = 8 * 1024**2

    if file_size > FILE_SIZE_LIMIT:
        raise ValueError(
            f"""Provided file size of {file_size / 1024**4:2.f}TiB
            exceeds maximum allowed file size of 5 TiB"""
        )

    # clamp to lower/upper bound, respectively
    if preferred_part_size < LOWER_BOUND:
        preferred_part_size = LOWER_BOUND
    elif preferred_part_size > UPPER_BOUND:
        preferred_part_size = UPPER_BOUND

    # powers of two from 8 MiB to 1024 MiBs, everything above would produce files
    # over the limit
    part_size_candidates = [2**i * 1024**2 for i in range(3, 11)]

    if file_size / preferred_part_size > MAX_NUM_PARTS:
        for part_size_candidate in part_size_candidates:
            if file_size / part_size_candidate <= MAX_NUM_PARTS:
                return part_size_candidate
    return preferred_part_size


def check_ascii(*str_values: str):
    """Checks that the provided `str_values` are ASCII compatible,
    raises an exception otherwise.
    """
    for str_value in str_values:
        if not str_value.isascii():
            raise NonAsciiStrError(str_value=str_value)


def validate_fields_in_model(
    *,
    model: type[BaseModel],
    fields: Collection[str],
) -> None:
    """Checks that all provided fields are present in the dto_model.
    Raises IndexFieldsInvalidError otherwise.
    """
    fields_set = set(fields)
    existing_fields_set = set(model.model_json_schema()["properties"])

    if not fields_set.issubset(existing_fields_set):
        unexpected_fields = fields_set.difference(existing_fields_set)
        raise FieldNotInModelError(model=model, unexpected_field=unexpected_fields)


@asynccontextmanager
async def set_context_var(context_var: ContextVar, value: Any):
    """An async context manager to simplify the use of ContextVars.

    The value will be reset upon exiting the context.
    """
    token = context_var.set(value)
    yield
    context_var.reset(token)


def round_datetime_to_ms(dt: datetime) -> datetime:
    """Round a datetime object to nearest millisecond."""
    microseconds = dt.microsecond
    sub_ms = microseconds % 1000
    if not sub_ms:
        return dt

    # Calculate the delta to add or subtract to round to the nearest millisecond.
    # we subtract sub_ms either from 1000 if rounding up or from 0 if rounding down
    delta = timedelta(microseconds=(1000 * (sub_ms >= 500)) - sub_ms)
    return dt + delta


def now_utc_ms_prec() -> datetime:
    """Return the current UTC time with microseconds rounded to milliseconds.

    This is useful for producing a datetime that is consistent
    with MongoDB's millisecond precision.
    """
    current_time = datetime.now(timezone.utc)
    # Round microseconds to milliseconds
    return round_datetime_to_ms(current_time)

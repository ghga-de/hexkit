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

"""Utilities related to correlation IDs"""

import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from uuid import UUID, uuid4

from hexkit.utils import set_context_var

log = logging.getLogger(__name__)

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="")

__all__ = [
    "CorrelationIdContextError",
    "InvalidCorrelationIdError",
    "get_correlation_id",
    "new_correlation_id",
    "set_correlation_id",
    "set_new_correlation_id",
    "validate_correlation_id",
]


class CorrelationIdContextError(RuntimeError):
    """Raised when the correlation ID ContextVar is unexpectedly not set."""

    def __init__(self):
        super().__init__("No correlation ID has been set.")


class InvalidCorrelationIdError(RuntimeError):
    """Raised when a correlation ID fails validation."""

    def __init__(self, *, correlation_id: str):
        message = f"Invalid correlation ID found: '{correlation_id}'"
        super().__init__(message)


def new_correlation_id() -> str:
    """Generates a new correlation ID."""
    return str(uuid4())


def validate_correlation_id(correlation_id: str):
    """Raises an error if the correlation ID is invalid.

    Raises:
        InvalidCorrelationIdError: If the correlation ID is empty or invalid.
    """
    try:
        UUID(correlation_id)
    except ValueError as err:
        raise InvalidCorrelationIdError(correlation_id=correlation_id) from err


@asynccontextmanager
async def set_correlation_id(correlation_id: str):
    """Set the given correlation ID for the life of the context.

    Raises:
        InvalidCorrelationIdError: when the correlation ID is empty or invalid.
    """
    validate_correlation_id(correlation_id)

    async with set_context_var(correlation_id_var, correlation_id):
        log.info("Set context correlation ID to %s", correlation_id)
        yield


@asynccontextmanager
async def set_new_correlation_id():
    """Set a new correlation ID for the life of the context."""
    correlation_id = new_correlation_id()

    async with set_context_var(correlation_id_var, correlation_id):
        log.info("Set context correlation ID to %s", correlation_id)
        yield correlation_id


def get_correlation_id() -> str:
    """Get the correlation ID.

    This should only be called when the correlation ID ContextVar is expected to be set.

    Raises:
        CorrelationIdContextError: when the correlation ID ContextVar is not set.
        InvalidCorrelationIdError: when the correlation ID is invalid.
    """
    if not (correlation_id := correlation_id_var.get()):
        raise CorrelationIdContextError()

    validate_correlation_id(correlation_id)

    return correlation_id

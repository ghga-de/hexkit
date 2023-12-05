# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Test to verify correlation ID functionality."""

import asyncio
import random
from contextlib import nullcontext
from contextvars import ContextVar

import pytest

from hexkit.correlation import (
    CorrelationIdContextError,
    InvalidCorrelationIdError,
    correlation_id_var,
    get_correlation_id,
    set_correlation_id,
    validate_correlation_id,
)
from hexkit.utils import set_context_var

# Set seed to avoid non-deterministic outcomes with random.random()
random.seed(17)

VALID_CORRELATION_ID = "7041eb31-7333-4b57-97d7-90f5562c3383"


async def set_id_sleep_resume(correlation_id: str):
    """An async task to set the correlation ID ContextVar and yield control temporarily
    back to the event loop before resuming.
    Test with a sleep time of 0-2s and a random combination of context
    manager/directly setting ContextVar.
    """
    use_context_manager = random.choice((True, False))
    if use_context_manager:
        async with set_context_var(correlation_id_var, correlation_id):
            await asyncio.sleep(random.random() * 2)  # Yield control to the event loop
            # Check if the correlation ID is still the same
            assert correlation_id_var.get() == correlation_id, "Correlation ID changed"

        # make sure value is reset after exiting context manager
        assert correlation_id_var.get() == ""
    else:
        token = correlation_id_var.set(correlation_id)  # Set correlation ID for task
        await asyncio.sleep(random.random() * 2)  # Yield control to the event loop
        # Check if the correlation ID is still the same
        assert correlation_id_var.get() == correlation_id, "Correlation ID changed"
        correlation_id_var.reset(token)


@pytest.mark.asyncio
async def test_correlation_id_isolation():
    """Make sure correlation IDs are isolated to the respective async task and that
    there's no interference from task switching.
    """
    tasks = [set_id_sleep_resume(f"test_{n}") for n in range(100)]
    await asyncio.gather(*tasks)


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("BAD_ID", InvalidCorrelationIdError),
        ("", InvalidCorrelationIdError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_correlation_id_validation(correlation_id: str, exception):
    """Ensure an error is raised when correlation ID validation fails."""
    with pytest.raises(exception) if exception else nullcontext():
        validate_correlation_id(correlation_id)


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("12345", InvalidCorrelationIdError),
        ("", InvalidCorrelationIdError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_set_correlation_id(correlation_id: str, exception):
    """Ensure correct error is raised when passing an invalid or empty string to
    `set_correlation_id`.
    """
    with pytest.raises(exception) if exception else nullcontext():
        async with set_correlation_id(correlation_id=correlation_id):
            pass


@pytest.mark.parametrize(
    "correlation_id,exception",
    [
        ("12345", InvalidCorrelationIdError),
        ("", CorrelationIdContextError),
        (VALID_CORRELATION_ID, None),
    ],
)
@pytest.mark.asyncio
async def test_get_correlation_id(correlation_id: str, exception):
    """Ensure an error is raised when calling `get_correlation_id` for an empty id or
    invalid ID.
    """
    async with set_context_var(correlation_id_var, correlation_id):
        with pytest.raises(exception) if exception else nullcontext():
            get_correlation_id()


@pytest.mark.asyncio
async def test_context_var_setter():
    """Make sure `set_context_var()` properly resets the context var after use."""
    default = "default"
    outer_value = "outer"
    inner_value = "inner"
    test_var: ContextVar[str] = ContextVar("test", default=default)

    # Make sure the initial `get()` returns the default value
    assert test_var.get() == default

    # Ensure the value is set in the context manager
    async with set_context_var(test_var, outer_value):
        assert test_var.get() == outer_value

        # Ensure the value that is reset is actually the previous value, not just default
        async with set_context_var(test_var, inner_value):
            assert test_var.get() == inner_value
        assert test_var.get() == outer_value

    # Ensure the set value is removed/cleaned up by the function
    assert test_var.get() == default

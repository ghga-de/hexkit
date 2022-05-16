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

"""Test utilities from the `inject` module."""

from contextlib import nullcontext
from typing import Optional

import dependency_injector.containers
import dependency_injector.providers
import pytest

from hexkit.custom_types import ContextConstructable
from hexkit.inject import ContextConstructor
from tests.fixtures.inject import (
    NoCMConstructable,
    NoMethodConstructable,
    ValidConstructable,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constructable, exception",
    [
        (ValidConstructable, None),
        (NoCMConstructable, None),  # passes resources are not initialized in this test
        (object(), TypeError),
        (NoMethodConstructable, TypeError),
    ],
)
async def test_context_constructor_init(
    constructable: ContextConstructable, exception: Optional[Exception]
):
    """
    Test the initialization of a context constructor with valid and invalid
    constructables.
    """

    with pytest.raises(exception) if exception else nullcontext():
        test = ContextConstructor(constructable)

    if not exception:
        isinstance(test, dependency_injector.providers.Resource)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constructable, exception",
    [
        (ValidConstructable, None),
        (NoCMConstructable, TypeError),
    ],
)
async def test_context_constructor_setup_teardown(
    constructable: ContextConstructable, exception: Optional[Exception]
):
    """Test whether init and shutdown correctly works with a context constructor."""

    foo = "bar"

    test = ContextConstructor(constructable, foo)

    with pytest.raises(exception) if exception else nullcontext():
        await test()
        test_instance = await test.init()

        assert test_instance.foo == foo
        assert test_instance.in_context

        await test.shutdown()
        assert not test_instance.in_context

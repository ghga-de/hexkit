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

from hexkit.custom_types import AsyncContextConstructable
from hexkit.inject import (
    AsyncConstructor,
    NotConstructableError,
    assert_async_constructable,
    get_constructor,
)
from tests.fixtures.inject import (
    NoAsyncConstructable,
    NoMethodConstructable,
    NonResource,
    ValidAsyncConstructable,
    ValidAsyncContextConstructable,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constructable, does_raises",
    [
        (ValidAsyncContextConstructable, False),
        (ValidAsyncConstructable, False),
        (NoAsyncConstructable, False),
        # The above does not raise an exception even though the `construct` method does
        # not return an async context manager. This is a limitation of the current
        # implementation.
        (object(), True),
        (NoMethodConstructable, True),
    ],
)
async def test_assert_constructable(
    constructable: type[AsyncContextConstructable], does_raises: bool
):
    """
    Test that assert_constructable can distinguish between
    """

    with pytest.raises(NotConstructableError) if does_raises else nullcontext():  # type: ignore
        assert_async_constructable(constructable)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constructable, exception",
    [
        (ValidAsyncConstructable, None),
        (ValidAsyncContextConstructable, None),
        (
            NoAsyncConstructable,
            None,
        ),  # passed resources are not initialized in this test
        (object(), NotConstructableError),
        (NoMethodConstructable, NotConstructableError),
    ],
)
async def test_context_constructor_init(
    constructable: type[AsyncContextConstructable], exception: Optional[type[Exception]]
):
    """
    Test the initialization of a context constructor with valid and invalid
    constructables.
    """

    with pytest.raises(exception) if exception else nullcontext():  # type: ignore
        test = AsyncConstructor(constructable)

    if not exception:
        isinstance(test, dependency_injector.providers.Resource)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constructable, exception, has_context",
    [
        (ValidAsyncConstructable, None, False),
        (ValidAsyncContextConstructable, None, True),
        (NoAsyncConstructable, NotConstructableError, True),
    ],
)
async def test_context_constructor_setup_teardown(
    constructable: type[AsyncContextConstructable],
    exception: Optional[type[Exception]],
    has_context: bool,
):
    """Test whether init and shutdown correctly works with a context constructor."""

    foo = "bar"

    test = AsyncConstructor(constructable, foo)

    with pytest.raises(exception) if exception else nullcontext():  # type: ignore
        resource = await test.async_()
        assert isinstance(resource, constructable)
        test_instance = await test.init()  # type: ignore

        assert test_instance.foo == foo

        if has_context:
            assert test_instance.in_context

        await test.shutdown()  # type: ignore

        if has_context:
            assert not test_instance.in_context


@pytest.mark.parametrize(
    "provides, args, kwargs, constructor_cls",
    [
        (ValidAsyncConstructable, [], {}, AsyncConstructor),
        (ValidAsyncContextConstructable, [], {}, AsyncConstructor),
        (NonResource, ["foo"], {"bar": "bar"}, dependency_injector.providers.Factory),
    ],
)
def test_get_constructor(provides: type, args, kwargs, constructor_cls: type):
    """Tests whether the `get_constructor` function chooses the correct constructor
    classes for the given `provides` classes."""

    constructor = get_constructor(provides, *args, **kwargs)

    assert isinstance(constructor, constructor_cls)

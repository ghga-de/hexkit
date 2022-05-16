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

import dependency_injector.containers
import dependency_injector.providers
import pytest

from hexkit.inject import AsyncInitShutdownError, ContainerBase, ContextConstructor
from tests.fixtures.inject import ValidConstructable, ValidResource, ValidSyncResource


@pytest.mark.asyncio
async def test_context_constructor_with_decl_container():
    """
    Test the context constructor together with the `DeclarativeContainer` from the
    `dependency_injector` framework.
    """

    foo = "bar"

    class Container(dependency_injector.containers.DeclarativeContainer):
        test = ContextConstructor(ValidConstructable, foo)

    container = Container()
    await container.init_resources()  # type: ignore

    test_instance = await container.test()

    assert test_instance.foo == foo
    assert test_instance.in_context

    await container.shutdown_resources()  # type: ignore
    assert not test_instance.in_context


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provides, constructor",
    [
        (ValidResource, dependency_injector.providers.Resource),
        (ValidConstructable, ContextConstructor),
    ],
)
async def test_container_base(provides, constructor):
    """
    Test the ContainerBase and its contextual setup and teardown functionality.
    """

    foo = "bar"

    class Container(ContainerBase):
        test = constructor(provides, foo)

    async with Container() as container:

        test_instance = await container.test()

        assert test_instance.foo == foo
        assert test_instance.in_context

    assert not test_instance.in_context


@pytest.mark.asyncio
async def test_container_base_sync_resouce():
    """Make sure that using a non async Resource with the ContainerBase results in an
    exception."""

    class Container(ContainerBase):
        test = dependency_injector.providers.Resource(ValidSyncResource, "bar")

    container = Container()

    with pytest.raises(AsyncInitShutdownError):
        async with container:
            pass

    # also check the the __aenter__ and __aexit__ method separately:
    with pytest.raises(AsyncInitShutdownError):
        await container.__aenter__()

    with pytest.raises(AsyncInitShutdownError):
        await container.__aexit__(..., ..., ...)

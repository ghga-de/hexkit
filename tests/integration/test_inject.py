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

from contextlib import asynccontextmanager

import dependency_injector.containers
import dependency_injector.providers
import pytest
from pydantic import BaseSettings

from hexkit.inject import (
    AsyncInitShutdownError,
    ContainerBase,
    ContextConstructor,
    get_configurator,
    get_constructor,
)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("load_config", [True, False])
async def test_configurator(load_config: bool):
    """Test the configurator with default config parameters."""

    class ExampleConfig(BaseSettings):
        """A collection of example config parameter and their defaults."""

        param_a: str = "foo"
        param_b: int = 27
        param_c: bool = True

    # If load_config is false, the default config values are expected, otherwise a
    # custom set of values is used:
    expected_config = (
        ExampleConfig(param_a="bar", param_b=28) if load_config else ExampleConfig()
    )

    class SyncConfigConsumer:
        """A class that consumes an entire ExampleConfig instance (and not just
        individual parameters)."""

        def __init__(self, *, config: ExampleConfig):
            """Takes an ExampleConfig instance and checks their values against the
            expectation."""

            self.config = config

    class AsyncConfigConsumer(SyncConfigConsumer):
        """A class that consumes an entire ExampleConfig instance (and not just
        individual parameters). Is constucted using an async context manager."""

        @classmethod
        @asynccontextmanager
        async def construct(cls, *, config: ExampleConfig):
            """A constructor with setup and teardown logic.
            Just there so that we can use the container as an async context manager."""

            yield cls(config=config)

    class Container(ContainerBase):
        config = get_configurator(ExampleConfig)
        sync_config_consumer = get_constructor(SyncConfigConsumer, config=config)
        async_config_consumer = get_constructor(AsyncConfigConsumer, config=config)

    container_cm = Container()

    if load_config:
        container_cm.config.load_config(expected_config)
    async with container_cm as container:
        # Construct consumers:
        sync_config_consumer = container.sync_config_consumer()
        async_config_consumer = await container.async_config_consumer()

        # Check the consumed values:
        assert sync_config_consumer.config.dict() == expected_config.dict()
        assert async_config_consumer.config.dict() == expected_config.dict()

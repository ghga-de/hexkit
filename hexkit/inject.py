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

"""
This module contains a helper function that automatically selects the suitable
"Provider" class for the dependency_injector framework and makes the "Container" class
usable as async context manager.

Please Note:
To avoid overloading the "Provider" terminology of the Triple Hexagonal Architecture
pattern, we use the term `Constructor` to refer to objects that in dependency_injector
framework are called `Providers`.
(Also see: https://python-dependency-injector.ets-labs.org/providers/index.html)
"""

import inspect
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import Any, Callable, Generic, Optional, TypeVar

import dependency_injector.containers
import dependency_injector.providers
from pydantic import BaseSettings

from hexkit.custom_types import ContextConstructable

__all__ = [
    "get_constructor",
    "ContainerBase",
    "NotConstructableError",
    "ContextConstructor",
    "AsyncInitShutdownError",
    "Configurator",
]


class NotConstructableError(TypeError):
    """Thrown when a ContextConstructable expected but not obtained."""


class AsyncInitShutdownError(TypeError):
    """Thrown when a container has sync `init_resource` or `shutdown_resource` methods
    but coroutines are needed."""


def assert_context_constructable(constructable: type[ContextConstructable]):
    """
    Make sure that the provided object has a callable attribute `construct`.
    If this check passes, it can be seen as a strong indication that the provided object
    is compliant with our definition of a ContextConstructable. However, it does not
    check whether `construct` really returns an async context manager.
    """

    if not callable(getattr(constructable, "construct", None)):
        raise NotConstructableError(
            "ContextConstructable class must have a callable `construct` attribute."
        )


class ContextConstructor(dependency_injector.providers.Resource):
    """Maps an asynchronous context manager onto the Resource class from the
    `dependency_injector` framework."""

    @staticmethod
    def constructable_to_resource(
        constructable: type[ContextConstructable],
    ) -> Callable[..., AsyncIterator[Any]]:
        """
        Converts an async context manager to an async generator that is compatible
        with the Resource definition of the `dependency_injector` framework.
        """

        assert_context_constructable(constructable)

        async def resource(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            constructor = constructable.construct(*args, **kwargs)

            if not isinstance(constructor, AbstractAsyncContextManager):
                raise NotConstructableError(
                    "Callable attribute `construct` of ContextConstructable class must"
                    + " return an async context manager."
                )

            async with constructor as context:
                yield context

        return resource

    # This pylint error is inherited from the dependency_injector framework, other
    # DI providers use the same basic signitature:
    # pylint: disable=keyword-arg-before-vararg
    def __init__(
        self,
        provides: Optional[type[ContextConstructable]] = None,
        *args: dependency_injector.providers.Injection,
        **kwargs: dependency_injector.providers.Injection,
    ):
        """Initialize `dependency_injector`'s Resource with an AbstractAsyncContextManager."""

        if provides is None:
            super().__init__()
        else:
            resource = self.constructable_to_resource(provides)
            super().__init__(resource, *args, **kwargs)


def get_constructor(provides: type, *args, **kwargs):
    """Automatically selects and applies the right constructor for the class given to
    `provides`."""

    constructor_cls: type

    try:
        assert_context_constructable(provides)
    except TypeError:
        # `provides` is not a ContextConstructable
        constructor_cls = dependency_injector.providers.Factory
    else:
        # `provides` is a ContextConstructable
        constructor_cls = ContextConstructor

    return constructor_cls(provides, *args, **kwargs)


class CMDynamicContainer(dependency_injector.containers.DynamicContainer):
    """Adds a async context manager interface to the DynamicContainer base class from
    the `dependency_injector` framework."""

    async def __aenter__(self):
        """Init/setup resources."""

        init_future = self.init_resources()

        if not inspect.isawaitable(init_future):
            raise AsyncInitShutdownError(
                "Container does not support async initialization of resources."
            )

        await init_future
        return self

    async def __aexit__(self, exc_type, exc_value, exc_trace):
        """Shutdown/teardown resources"""

        shutdown_future = self.shutdown_resources()

        if not inspect.isawaitable(shutdown_future):
            raise AsyncInitShutdownError(
                "Container does not support async shutdown of resources."
            )

        await shutdown_future


SELF = TypeVar("SELF")


class ContainerBase(dependency_injector.containers.DeclarativeContainer):
    """
    A base container for dependency injection that handles init/setup and
    shutdown/teardown of resources via the async context manager protocol.
    """

    instance_type = CMDynamicContainer

    # Stubs to convince type checkers that this object is an async context manager,
    # even though the corresponding logic is only implemented in self.instance_type:
    # (See the implementation of the dependency_injector.containers.DeclarativeContainer
    # for more details.)

    async def __aenter__(self: SELF) -> SELF:
        """Init/setup resources."""
        ...

    async def __aexit__(self, exc_type, exc_value, exc_trace):
        """Shutdown/teardown resources"""
        ...


PydanticConfig = TypeVar("PydanticConfig", bound=BaseSettings)


class Configurator(dependency_injector.providers.Factory, Generic[PydanticConfig]):
    """A configuration constructor that holds configuration parameters using a pydantic
    model.

    Please note: While this is specific for reflecting"""

    def load_config(self, config: PydanticConfig):
        """"""

        self.override(dependency_injector.providers.Callable(lambda: config))

    def resolve(self):
        """"""

        if len(self.overridden) > 0:
            if isinstance(self.last_overriding, dependency_injector.providers.Callable):
                return self.last_overriding.provides()

            raise RuntimeError(
                "A Configurator should only be overwritten with a provider of type"
                + " 'dependency_injector.providers.Callable' but got: "
                + str(type(self.last_overriding))
            )

        return self.provides()

    def get(self, param_name: str):

        return dependency_injector.providers.Callable(
            lambda: getattr(self.resolve(), param_name)
        )

    def __getattr__(self, attr):

        if attr.startswith("__") and attr.endswith("__"):
            return super().__getattr__(attr)

        return self.get(attr)


def get_configurator(
    pydantic_cls: type[PydanticConfig],
) -> Configurator[PydanticConfig]:
    """Automatically selects and applies the right constructor for the class given to
    `provides`."""

    return Configurator[PydanticConfig](pydantic_cls)

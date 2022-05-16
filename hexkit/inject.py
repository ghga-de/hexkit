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
Utilities for dependency injection based on the dependency_injector framework.

Please Note:
To avoid overloading the "Provider" terminology of the Triple Hexagonal Architecture
pattern, we use the term `Constructor` to refer to objects that in dependency_injector
framework are called `Providers`.
(Also see: https://python-dependency-injector.ets-labs.org/providers/index.html)
"""

import inspect
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Optional,
    TypeVar,
    cast,
)

import dependency_injector.containers
import dependency_injector.providers

from hexkit.custom_types import ContextConstructable


def assert_context_constructable(constructable: type[ContextConstructable]):
    """
    Makes sure that the provided object has a callable attribute `construct` attribute.
    If this check passes, it can be seen as a strong indication that the provided object
    is compliant with our definition of a ContextConstructable.
    However, it does not checked whether the callable `construct` method returns an
    async context manager.
    """

    if not (
        hasattr(constructable, "construct")
        # Using isinstance together with Callable should be supported,
        # see: https://peps.python.org/pep-0484/#callable
        # Thus ignoring mypy error:
        and isinstance(constructable.construct, Callable)  # type: ignore
    ):
        raise TypeError(
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
        Converts an async context manager to an async Generator that is compatible
        with Resource definition of the `dependency_injector` framework.
        """

        assert_context_constructable(constructable)

        async def resource(*args: Any, **kwargs: Any) -> AsyncIterator[Any]:
            constructor = constructable.construct(*args, **kwargs)

            if not isinstance(constructor, AsyncContextManager):
                raise TypeError(
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
        **kwargs: dependency_injector.providers.Injection
    ):
        """Initialize `dependency_injector`'s Resource with an AsyncContextManager."""

        if provides is None:
            super().__init__()
        else:
            resource = self.constructable_to_resource(provides)
            super().__init__(resource, *args, **kwargs)


def get_constructor(provides: type, *args, **kwargs):
    """Automatically selects and applys the right constructor for the class given to
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

        if not hasattr(self, "init_resources") and inspect.iscoroutine(
            self.init_resources
        ):
            raise TypeError(
                "Container does not support async initialization of resources."
            )

        init_resources = cast(Callable[[], Awaitable], self.init_resources)

        await init_resources()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_trace):
        """Shutdown/teardown resources"""

        if not hasattr(self, "shutdown_resources") and inspect.iscoroutine(
            self.shutdown_resources
        ):
            raise TypeError("Container does not support async shutdown of resources.")

        shutdown_resources = cast(Callable[[], Awaitable], self.shutdown_resources)

        await shutdown_resources()


SELF = TypeVar("SELF")


class ContainerBase(dependency_injector.containers.DeclarativeContainer):
    """
    A base container for dependency injection that handles init/setup and
    shutdown/teardown of resources via the async context manager protocol.
    """

    instance_type = CMDynamicContainer

    # Stubs to convince type checkers that this object is an async context manager,
    # event though the corresponding logic is only implemented in self.instance_type:
    # (See the implementation of the dependency_injector.containers.DeclarativeContainer
    # for more details.)

    async def __aenter__(self: SELF) -> SELF:
        """Init/setup resources."""
        ...

    async def __aexit__(self, exc_type, exc_value, exc_trace):
        """Shutdown/teardown resources"""
        ...

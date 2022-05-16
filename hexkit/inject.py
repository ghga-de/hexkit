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

"""Utilities for dependency injection based on the dependency_injector framework."""

from typing import Any, AsyncContextManager, AsyncIterator, Callable, Optional

import dependency_injector.providers
import dependency_injector.resources

from hexkit.custom_types import ContextConstructable


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

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

"""Utility wrappers around functionality from the dependency_injector framework"""

from abc import ABC
from typing import AsyncGenerator, TypeVar

import dependency_injector.containers
import dependency_injector.providers
import dependency_injector.resources

from hexkit.custom_types import AsyncClosable


def closable_to_resource(target_cls: type[AsyncClosable]) -> AsyncGenerator:
    """wraps a Closable to be compatible with the Resource provider from the
    dependency injector framework."""

    async def wrapper(**kwargs):
        try:
            target = await target_cls(**kwargs)
            yield target
        finally:
            target.close()

    return wrapper


TC = TypeVar("TC")  # Type variable for the target class of the Factory


class Resource(dependency_injector.providers.Resource[TC]):
    """
    Extends the `Resource` provider (not hexagonal) from the depenedency.injector
    so that it recognizes the `__init__` and the `close` methods (need to exist) of a
    target class as `init` and `shutdown`, respectively, as per the Resource terminology
    (see here:
    https://python-dependency-injector.ets-labs.org/providers/resource.html#subclass-initializer).
    """

    async def __init__(self, target_cls: type[AsyncClosable], **kwargs):
        resource_compatible = closable_to_resource(target_cls)  # a coroutine

        super().__init__(resource_compatible, **kwargs)


class ContainerBase(dependency_injector.containers.DeclarativeContainer, ABC):
    """A dependency injection container that can act as context manager for savely
    initialize and shutting down Resources."""

    def __enter__(self):
        """Initialize Resourses"""
        self.init_resources()
        return self

    def __exit__(self, ex_type, ex_val, ex_trace):
        """Shutdown Resources."""
        self.shutdown_resources()

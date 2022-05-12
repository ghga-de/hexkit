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

"""Collection of base classes."""

from abc import ABC, abstractmethod
from contextlib import contextmanager


class ResourceProvider(ABC):
    """An abstract base class that defines (abstract) methods for using a 3-hex provider
    as a `Resource` according to the `dependency_injector` framework.
    Please see here for more information on resources:
    """

    @classmethod
    @contextmanager
    def as_context_manager(cls, **kwargs):
        """Generate a provider instance as resource provider.
        Please provide all keyword arguments (non-keyword arguments are not allowed)
        needed for the __init__ method.
        """
        resource = cls.as_resource(**kwargs)
        for provider in resource:
            yield provider

    @classmethod
    def as_resource(cls, **kwargs):
        """Generate a provider instance as resource provider.
        Please provide all keyword arguments (non-keyword arguments are not allowed)
        needed for the __init__ method.
        """
        self = cls(**kwargs)
        yield self
        self.close()

    def close(self) -> None:
        """Close/teardown the instance."""
        # No teardown logic by default. Please overwrite if required.


class InboundProviderBase(ResourceProvider, ABC):
    """Base class that should be used by inbound providers."""

    @abstractmethod
    def run(self, forever: bool = True) -> None:
        """
        Runs the inbound provider. Typically, it blocks forever.
        However, you can set `forever` to `False` to make it return after handling one
        operation.
        """
        ...


class OutboundProviderBase(ResourceProvider, ABC):
    """Base class that should be used by outbound providers."""

    # Additional logic might be added in the future, for now identical with the
    # ResourceProvider

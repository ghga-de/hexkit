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

"""Test base classes."""
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Resource

from hexkit.base import ResourceProvider


class TestProvider(ResourceProvider):
    """A test implemenation of a resource provider."""

    def __init__(self, *, some_kwarg: str, another_kwarg: int) -> None:
        """Initialise the provider with some (keyword) arguments."""
        self.some_kwarg = some_kwarg
        self.another_kwarg = another_kwarg
        self.close_method_called = False

    def close(self) -> None:
        """Close/teardown the test povider instance."""
        self.close_method_called = True


SOME_KWARG = "test"
ANOTHER_KWARG = 123456


def test_resource_provider_for_di():
    """Test the resource provider as dependency_injector resource."""

    class Container(DeclarativeContainer):
        """Dependency injection container."""

        test_provider = Resource(
            TestProvider.as_resource, some_kwarg=SOME_KWARG, another_kwarg=ANOTHER_KWARG
        )

    container = Container()
    container.init_resources()
    test_provider = container.test_provider()

    # make sure that the test provider was correctly initialized:
    assert test_provider.some_kwarg == SOME_KWARG
    assert test_provider.another_kwarg == ANOTHER_KWARG
    assert not test_provider.close_method_called

    container.shutdown_resources()

    # make sure that the test provider was correctly closed:
    assert test_provider.close_method_called


def test_resource_provider_as_cm():
    """Test resource provider as context manager."""

    with TestProvider.as_context_manager(
        some_kwarg=SOME_KWARG, another_kwarg=ANOTHER_KWARG
    ) as test_provider:
        # make sure that the test provider was correctly initialized:
        assert test_provider.some_kwarg == SOME_KWARG
        assert test_provider.another_kwarg == ANOTHER_KWARG
        assert not test_provider.close_method_called

    assert test_provider.close_method_called

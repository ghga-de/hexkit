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

from hexkit.inject import ContextConstructor
from tests.fixtures.inject import ValidConstructable


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
    await container.init_resources()

    test_instance = await container.test()

    assert test_instance.foo == foo
    assert test_instance.in_context

    await container.shutdown_resources()
    assert not test_instance.in_context

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

"""Fixtures and utils for testing the `inject` module."""


from contextlib import asynccontextmanager
from typing import Optional


class ValidConstructable:
    """A test class with a `construct` method that is an async context manager."""

    @classmethod
    @asynccontextmanager
    async def construct(cls, foo: str = "foo"):
        """A constructor with setup and teardown logic."""
        try:
            instance = cls(foo=foo)
            yield instance
        finally:
            instance.in_context = False

    def __init__(self, foo: str = "foo"):
        """Init TestConstructable."""
        self.foo: Optional[str] = foo
        self.in_context = True


class NoMethodConstructable:
    """
    A non valid ContextConstructable:
    has a `construct` attribute, however, it's not a callable.
    """

    construct = "invalid"


class NoCMConstructable:
    """
    A non valid ContextConstructable:
    has a `construct` method which, however, does not return an async context manager.
    """

    @classmethod
    async def construct(cls, foo: str = "foo"):
        """A constructor with setup and teardown logic."""
        try:
            instance = cls(foo=foo)
            yield instance
        finally:
            instance.in_context = False

    def __init__(self, foo: str = "foo"):
        """Init TestConstructable."""
        self.foo: Optional[str] = foo
        self.in_context = True

# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Utilities for testing that aren't associated with any particular tool"""

import asyncio

import pytest_asyncio
from pytest_asyncio.plugin import _ScopeName


def event_loop_fixture():
    """Event loop fixture for when an event loop is needed beyond function scope.

    **Do not call directly** Instead, use get_event_loop()
    """
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


def get_event_loop(scope: _ScopeName):
    """Return an event loop fixture"""
    return pytest_asyncio.fixture(fixture_function=event_loop_fixture, scope=scope)

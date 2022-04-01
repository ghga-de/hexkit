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

"""General utilities that don't require heavy dependencies."""

import os
import signal
from pathlib import Path
from typing import Callable, Optional

from pydantic import BaseSettings

TEST_FILE_DIR = Path(__file__).parent.resolve() / "test_files"

TEST_FILE_PATHS = [
    TEST_FILE_DIR / filename
    for filename in os.listdir(TEST_FILE_DIR)
    if filename.startswith("test_") and filename.endswith(".yaml")
]


class OutOfContextError(RuntimeError):
    """Thrown when a context manager is used out of context."""

    def __init__(self):
        message = "Used context manager outside of a with block."
        super().__init__(message)


class DaoGenericBase:
    """A generic base for implementing DAO interfaces."""

    def __init__(self, config: BaseSettings):  # pylint: disable=unused-argument
        """Initialize DAO with a config params passed as pydantic BaseSettings object."""
        ...

    # Every DAO must support the context manager protocol.
    # However, it is up to the DAO implementation whether
    # the `__enter__` and `__exit__` functions are doing
    # something useful. They may remain stubs:

    def __enter__(self):
        """Setup logic."""
        ...
        return self

    def __exit__(self, err_type, err_value, err_traceback):
        """Teardown logic."""
        ...


# pylint: disable=too-few-public-methods
class AsyncDaoGenericBase:
    """A generic base for implementing an asynchronous DAO interfaces."""

    def __init__(self, config: BaseSettings):  # pylint: disable=unused-argument
        """Initialize DAO with a config params passed as pydantic BaseSettings object."""
        ...

    # Every DAO must support the context manager protocol.
    # However, it is up to the DAO implementation whether
    # the `__enter__` and `__exit__` functions are doing
    # something useful. They may remain stubs:

    async def __aenter__(self):
        """Setup logic."""
        ...
        return self

    # pylint: disable=unused-argument,no-self-use
    async def _aexit__(self, err_type, err_value, err_traceback):
        """Teardown logic."""
        ...


def raise_timeout_error(_, __):
    """Raise a TimeoutError"""
    raise TimeoutError()


def exec_with_timeout(
    func: Callable,
    timeout_after: int,
    func_args: Optional[list] = None,
    func_kwargs: Optional[dict] = None,
):
    """
    Exec a function (`func`) with a specified timeout (`timeout_after` in seconds).
    If the function doesn't finish before the timeout, a TimeoutError is thrown.
    """

    func_args_ = [] if func_args is None else func_args
    func_kwargs_ = {} if func_kwargs is None else func_kwargs

    # set a timer that raises an exception if timed out
    signal.signal(signal.SIGALRM, raise_timeout_error)
    signal.alarm(timeout_after)

    # execute the function
    result = func(*func_args_, **func_kwargs_)

    # disable the timer
    signal.alarm(0)

    return result


def create_fake_drs_uri(object_id: str):
    """Create a fake DRS URI based on an object id."""
    return f"drs://www.example.org/{object_id}"

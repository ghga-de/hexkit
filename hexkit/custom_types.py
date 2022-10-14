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

"""Module collecting custom types."""

from collections.abc import Mapping, Sequence
from typing import Any, Union

# This is intended to type objects that could be a potential output of `json.loads`.
# (Scalar types as well as arrays are excluded from the above assumption.)
JsonObject = Mapping[
    str, Union[int, float, str, bool, Sequence[Any], Mapping[str, Any]]
]


# A type indicating that a string should be ascii-compatible.
# Technically it is an alias for `str` so it only serves documention purposes.
Ascii = str


# A AsyncConstructable is a class with a async (class-)method `construct` that is used when
# asynchronous constuction/instantiation logic is needed (which cannot be handeled in
# a synchronous __init__ method).
# With the current typing features of Python, it seems not possible to correctly type
# a class with that signature.
# E.g. one could define a typing.Protocol subclass with following method:
# ```
# class AsyncConstructable(Protocol):
#     @classmethod
#     async def construct(cls, *args: Any, **kwargs: Any): ...
# ```
# However, this is incompatible with implementations that don't explicitly use `*args`
# and `*kwargs`, e.g. the following method does not comply with the above function stub:
# ```
# class SomeImplementation(AsyncConstructable):
#     @classmethod
#     async def construct(cls, foo: str): ...
# ```
# Thus using a type alias for now:
AsyncConstructable = Any


# A AsyncContextConstructable is a class with a (class-)method `construct` that creates an async
# context manager for safely setting up and tearing down an instance of that class.
# With the current typing features of Python, it seems not possible to correctly type
# a class with that signature.
# E.g. one could define a typing.Protocol subclass with following method:
# ```
# class AsyncContextConstructable(Protocol):
#     @classmethod
#     @asynccontextmanager
#     async def construct(cls, *args: Any, **kwargs: Any): ...
# ```
# However, this is incompatible with implementations that don't explicitly use `*args`
# and `*kwargs`, e.g. the following method does not comply with the above function stub:
# ```
# class SomeImplementation:
#     @classmethod
#     @asynccontextmanager
#    async def construct(cls, foo: str): ...
# ```
# Thus using a type alias for now:
AsyncContextConstructable = Any

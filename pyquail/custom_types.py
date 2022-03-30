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

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Type, Union

if TYPE_CHECKING:

    class JSONArray(list["JSON"], Protocol):  # type: ignore
        """For use in type hinting."""

        __class__: Type[list["JSON"]]  # type: ignore

    class JSONObject(dict[str, "JSON"], Protocol):  # type: ignore
        """For use in type hinting."""

        __class__: Type[dict[str, "JSON"]]  # type: ignore

    JSON = Union[None, float, str, JSONArray, JSONObject]

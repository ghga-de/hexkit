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

from typing import Any, Mapping, Union

# This is intended to type objects that could be a potential output of `json.loads`.
# (Scalar types as well as arrays are excluded from the above assumption.)
JsonObject = Mapping[str, Union[int, float, str, bool, list[Any], Mapping[str, Any]]]


# A type indicating that a string should be ascii-compatible.
# Technically it is an alias for `str` so it only serves documention purposes.
Ascii = str

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

"""Test functionality in the utils package."""

from contextlib import nullcontext
from typing import Optional

import pytest

from hexkit.utils import NonAsciiStrError, check_ascii


@pytest.mark.parametrize(
    "str_values, exception",
    [
        (["valid"], None),
        (["invälid"], NonAsciiStrError),
        (["valid", "also_valid_123-?$3&"], None),
        (["valid", "invälid"], NonAsciiStrError),
    ],
)
def test_check_ascii(str_values: list[str], exception: Optional[type[Exception]]):
    """Test the check_ascii function"""
    with pytest.raises(exception) if exception else nullcontext():  # type: ignore
        check_ascii(*str_values)

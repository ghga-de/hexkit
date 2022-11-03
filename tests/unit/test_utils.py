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

from collections.abc import Collection
from contextlib import nullcontext

import pytest
from pydantic import BaseModel

from hexkit.utils import (
    FieldNotInModelError,
    NonAsciiStrError,
    calc_part_size,
    check_ascii,
    validate_fields_in_model,
)

MiB = 1024**2
GiB = 1024**3
TiB = 1024**4


@pytest.mark.parametrize(
    "preferred_part_size, file_size, expected_part_size",
    [
        (16 * MiB, 10 * GiB, 16 * MiB),
        (16 * MiB, 200 * GiB, 32 * MiB),
        (4 * MiB, 10 * GiB, 8 * MiB),
        (6 * GiB, 10 * GiB, 4 * GiB),
        (16 * MiB, 10 * TiB, None),
        (None, 20 * MiB, 8 * MiB),
        (None, 10 * GiB, 8 * MiB),
        (None, 200 * GiB, 32 * MiB),
    ],
)
def test_calc_part_size(
    preferred_part_size: int, file_size: int, expected_part_size: int
):
    """Test code to dynamically adapt part size"""
    with pytest.raises(ValueError) if file_size > 5 * TiB else nullcontext():  # type: ignore
        adapted_part_size = calc_part_size(
            preferred_part_size=preferred_part_size, file_size=file_size
        )
        assert adapted_part_size == expected_part_size


@pytest.mark.parametrize(
    "str_values",
    (["valid"], ["valid", "also_valid_123-?$3&"]),
)
def test_check_ascii_happy(str_values: list[str]):
    """Test the check_ascii function with valid parameters."""
    check_ascii(*str_values)


@pytest.mark.parametrize(
    "str_values",
    (["invälid"], ["valid", "invälid"]),
)
def test_check_ascii_error(str_values: list[str]):
    """Test the check_ascii function with invalid parameters."""
    with pytest.raises(NonAsciiStrError):
        check_ascii(*str_values)


class ExampleModel(BaseModel):
    """An example pydantic model."""

    param_a: str
    param_b: int


@pytest.mark.parametrize("fields", ({"param_a"}, {"param_a", "param_b"}))
def test_validate_fields_in_model_happy(fields: Collection[str]):
    """Test validate_fields_in_model with valid parameters."""

    validate_fields_in_model(model=ExampleModel, fields=fields)


@pytest.mark.parametrize("fields", ({"param_c"}, {"param_a", "param_c"}))
def test_validate_fields_in_model_error(fields: Collection[str]):
    """Test validate_fields_in_model with invalid parameters."""

    with pytest.raises(FieldNotInModelError):
        validate_fields_in_model(model=ExampleModel, fields=fields)

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

from pydantic import BaseModel


class NonAsciiStrError(RuntimeError):
    """Thrown when non-ASCII string was unexpectedly provided"""

    def __init__(self, str_value: str):
        """Prepare custom message."""
        super().__init__(f"Non-ASCII string provided: {str_value!r}")


def check_ascii(*str_values: str):
    """Checks that the provided `str_values` are ASCII compatible,
    raises an exception otherwise."""
    for str_value in str_values:
        if not str_value.isascii():
            raise NonAsciiStrError(str_value=str_value)


class FieldNotInModelError(RuntimeError):
    """Raised when provided fields where not contained in a pydantic model."""

    def __init__(self, *, model: type[BaseModel], unexpected_field: set[str]):
        message = (
            f"The pydantic model {model} does not contain following fields: "
            + str(unexpected_field)
        )
        super().__init__(message)


def validate_fields_in_model(
    *,
    model: type[BaseModel],
    fields: set[str],
) -> None:
    """Checks that all provided fields are present in the dto_model.
    Raises IndexFieldsInvalidError otherwise."""

    existing_fields = set(model.schema()["properties"])

    if not fields.issubset(existing_fields):
        unexpected_fields = fields.difference(existing_fields)
        raise FieldNotInModelError(model=model, unexpected_field=unexpected_fields)

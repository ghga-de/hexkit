# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""MongoDB-based provider utilities.

Utilities for testing are located in `./testutils.py`.
"""

from contextlib import contextmanager
from typing import Any

from pymongo.errors import PyMongoError

from hexkit.protocols.dao import DaoError, DbTimeoutError, Dto

__all__ = ["document_to_dto", "dto_to_document", "translate_pymongo_errors"]


@contextmanager
def translate_pymongo_errors():
    """Catch PyMongoError and re-raise it as DbTimeoutError if it is a timeout error.

    Non-timeout errors are re-raised as DaoError.
    """
    try:
        yield
    except PyMongoError as exc:
        if exc.timeout:  # denotes timeout-related errors in pymongo
            raise DbTimeoutError(str(exc)) from exc
        raise DaoError(str(exc)) from exc


def document_to_dto(
    document: dict[str, Any], *, id_field: str, dto_model: type[Dto]
) -> Dto:
    """Converts a document obtained from the MongoDB database into a DTO model-
    compliant representation.
    """
    document[id_field] = document.pop("_id")
    return dto_model.model_validate(document)


def dto_to_document(dto: Dto, *, id_field: str) -> dict[str, Any]:
    """Converts a DTO into a representation that is a compatible document for a
    MongoDB Database.

    If there is a non-serializable field in the DTO, it will raise an error.
    Such DTOs should implement appropriate serialization methods or use a different
    data type for the field.
    """
    document = dto.model_dump()
    document["_id"] = document.pop(id_field)

    return document

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
#

"""A subpackage containing all MongoDB-specific providers.

Require dependencies of the `mongodb` extra.
"""

from .client import ConfiguredMongoClient
from .dao import (
    MongoDbDao,
    MongoDbDaoFactory,
    get_single_hit,
    replace_id_field_in_find_mapping,
    validate_find_mapping,
)
from .kvstore import (
    MongoDbBytesKeyValueStore,
    MongoDbDtoKeyValueStore,
    MongoDbJsonKeyValueStore,
    MongoDbStrKeyValueStore,
)
from .utils import document_to_dto, dto_to_document, translate_pymongo_errors

__all__ = [
    "ConfiguredMongoClient",
    "MongoDbBytesKeyValueStore",
    "MongoDbDao",
    "MongoDbDaoFactory",
    "MongoDbDtoKeyValueStore",
    "MongoDbJsonKeyValueStore",
    "MongoDbStrKeyValueStore",
    "document_to_dto",
    "dto_to_document",
    "get_single_hit",
    "replace_id_field_in_find_mapping",
    "translate_pymongo_errors",
    "validate_find_mapping",
]

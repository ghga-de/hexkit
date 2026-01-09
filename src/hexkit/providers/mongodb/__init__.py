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

"""Subpackage containing MongoDB-based providers and related utilities."""

from .config import MongoDbConfig
from .provider import (
    ConfiguredMongoClient,
    MongoDbBytesKeyValueStore,
    MongoDbDao,
    MongoDbDaoFactory,
    MongoDbDtoKeyValueStore,
    MongoDbJsonKeyValueStore,
    MongoDbStrKeyValueStore,
    translate_pymongo_errors,
)

__all__ = [
    "ConfiguredMongoClient",
    "MongoDbBytesKeyValueStore",
    "MongoDbConfig",
    "MongoDbDao",
    "MongoDbDaoFactory",
    "MongoDbDtoKeyValueStore",
    "MongoDbJsonKeyValueStore",
    "MongoDbStrKeyValueStore",
    "translate_pymongo_errors",
]

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

"""Providers that combine MongoDB and Kafka functionality"""

from .provider import (
    CHANGE_EVENT_TYPE,
    DELETE_EVENT_TYPE,
    MongoKafkaConfig,
    MongoKafkaDaoPublisherFactory,
    PersistentKafkaPublisher,
    document_to_dto,
    dto_to_document,
)

__all__ = [
    "CHANGE_EVENT_TYPE",
    "DELETE_EVENT_TYPE",
    "MongoKafkaConfig",
    "MongoKafkaDaoPublisherFactory",
    "PersistentKafkaPublisher",
    "document_to_dto",
    "dto_to_document",
]

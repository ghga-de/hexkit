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
"""MongoKafka-specific Config class"""

import logging

from pydantic import field_validator

from hexkit.providers.akafka.config import KafkaConfig
from hexkit.providers.mongodb.provider import MongoDbConfig


class MongoKafkaConfig(MongoDbConfig, KafkaConfig):
    """Config parameters and their defaults."""

    @field_validator("kafka_max_message_size", mode="after")
    @classmethod
    def validate_max_message_size(cls, value: int) -> int:
        """Validate the maximum message size."""
        if value > 2**24:  # 16 MiB
            logging.warning(
                f"Max message size ({value}) exceeds the 16 MiB document size limit for MongoDB!"
            )
        return value

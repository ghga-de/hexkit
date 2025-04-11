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
"""Unit tests related to the MongoKafka functionality."""

import logging

from pydantic import SecretStr

from hexkit.providers.mongokafka import MongoKafkaConfig


def make_mongokafka_config(kafka_max_message_size: int = 1048576) -> MongoKafkaConfig:
    """Create a MongoKafkaConfig object with a given kafka_max_message_size."""
    return MongoKafkaConfig(
        service_name="test",
        service_instance_id="1",
        kafka_servers=["localhost:9092"],
        db_connection_str=SecretStr("mongodb://localhost:27017"),
        db_name="test",
        kafka_max_message_size=kafka_max_message_size,
    )


def test_max_message_size_too_high(caplog):
    """Test for log message when kafka_max_message_size is over 16 MiB."""
    caplog.clear()
    limit = 16 * 1024 * 1024
    config = make_mongokafka_config(limit + 1)
    record = caplog.records[-1]
    assert record.levelno == logging.WARNING
    msg = "Max message size (16777217) exceeds the 16 MiB document size limit for MongoDB!"
    assert record.msg % record.args == msg
    assert config.kafka_max_message_size == limit + 1

    caplog.clear()
    config = make_mongokafka_config(limit)
    assert not caplog.records

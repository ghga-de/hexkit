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

"""Utilities related to Kafka."""

import ssl
from typing import Optional

from aiokafka.helpers import create_ssl_context

from hexkit.providers.akafka.config import KafkaConfig


def generate_client_id(*, service_name: str, instance_id: str) -> str:
    """
    Generate client id (from the perspective of the Kafka broker) by concatenating
    the service name and the client suffix.
    """
    return f"{service_name}.{instance_id}"


def generate_ssl_context(config: KafkaConfig) -> Optional[ssl.SSLContext]:
    """Generate SSL context for an encrypted SSL connection to Kafka broker."""
    return (
        create_ssl_context(
            cafile=config.kafka_ssl_cafile,
            certfile=config.kafka_ssl_certfile,
            keyfile=config.kafka_ssl_keyfile,
            password=config.kafka_ssl_password.get_secret_value(),
        )
        if config.kafka_security_protocol == "SSL"
        else None
    )

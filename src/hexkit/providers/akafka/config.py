# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Apache Kafka specific configuration."""

from typing import Literal

from pydantic import Field, PositiveInt, SecretStr
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """Config parameters needed for connecting to Apache Kafka."""

    service_name: str = Field(
        ...,
        examples=["my-cool-special-service"],
        description="The name of the (micro-)service from which messages are published.",
    )
    service_instance_id: str = Field(
        ...,
        examples=["germany-bw-instance-001"],
        description=(
            "A string that uniquely identifies this instance across all instances of"
            + " this service. A globally unique Kafka client ID will be created by"
            + " concatenating the service_name and the service_instance_id."
        ),
    )
    kafka_servers: list[str] = Field(
        ...,
        examples=[["localhost:9092"]],
        description="A list of connection strings to connect to Kafka bootstrap servers.",
    )
    kafka_security_protocol: Literal["PLAINTEXT", "SSL"] = Field(
        default="PLAINTEXT",
        description="Protocol used to communicate with brokers. "
        + "Valid values are: PLAINTEXT, SSL.",
    )
    kafka_ssl_cafile: str = Field(
        default="",
        description="Certificate Authority file path containing certificates"
        + " used to sign broker certificates. If a CA is not specified, the default"
        + " system CA will be used if found by OpenSSL.",
    )
    kafka_ssl_certfile: str = Field(
        default="",
        description="Optional filename of client certificate, as well as any"
        + " CA certificates needed to establish the certificate's authenticity.",
    )
    kafka_ssl_keyfile: str = Field(
        default="", description="Optional filename containing the client private key."
    )
    kafka_ssl_password: SecretStr = Field(
        default="",  # type: ignore
        description="Optional password to be used for the client private key.",
    )
    generate_correlation_id: bool = Field(
        default=True,
        examples=[True, False],
        description=(
            "A flag, which, if False, will result in an error when trying to publish an"
            + " event without a valid correlation ID set for the context. If True, the a"
            + " newly correlation ID will be generated and used in the event header."
        ),
    )
    kafka_max_message_size: PositiveInt = Field(
        default=1024 * 1024,  # 1 MiB
        description="The largest message size that can be transmitted, in bytes. Only"
        + " services that have a need to send/receive larger messages should set this.",
        examples=[1024 * 1024, 16 * 1024 * 1024],
    )

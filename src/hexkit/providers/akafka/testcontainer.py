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

"""Improved Kafka test containers."""

from textwrap import dedent
from typing import Literal, Optional

from testcontainers.kafka import KafkaContainer

__all__ = ["KafkaSSLContainer"]

DEFAULT_IMAGE = "confluentinc/cp-kafka:7.8.0"

DEFAULT_PORT = 9093  # default port for the Kafka container
BROKER_PORT = 9092  # auxiliary port for inter broker listener


class KafkaSSLContainer(KafkaContainer):
    """Kafka container that supports SSL (or actually TLS)."""

    SECRETS_PATH = "/etc/kafka/secrets"

    def __init__(  # noqa: C901, PLR0912, PLR0913
        self,
        image: str = DEFAULT_IMAGE,
        port: int = DEFAULT_PORT,
        cert: Optional[str] = None,
        key: Optional[str] = None,
        trusted: Optional[str] = None,
        password: Optional[str] = None,
        client_auth: Optional[Literal["requested", "required", "none"]] = None,
        **kwargs,
    ) -> None:
        """Initialize the Kafka SSL container with the given parameters.

        "cert" must contain the certificate of the broker and if needed also
        intermediate certificates. "key" must contain the private key of the
        broker. If it password  protected, "password" must be specified as well.
        "trusted" must contain the trusted certificates. In "client_auth" you can
        specify whether authentication is requested, required or not needed at all.
        """
        super().__init__(image, port, **kwargs)
        ssl = bool(cert or trusted or client_auth)
        protocol = "SSL" if ssl else "PLAINTEXT"
        self.protocol = protocol
        self.broker_port = DEFAULT_PORT if port == BROKER_PORT else BROKER_PORT
        self.listeners = (
            f"{protocol}://0.0.0.0:{port},BROKER://0.0.0.0:{self.broker_port}"
        )
        self.security_protocol_map = f"BROKER:PLAINTEXT,{protocol}:{protocol}"
        env = self.with_env
        env("KAFKA_LISTENERS", self.listeners)
        env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", self.security_protocol_map)
        if ssl:
            if cert:
                cert = cert.strip().replace("\n", "\\n")
            if key:
                key = key.strip().replace("\n", "\\n")
            if password:
                password = password.strip().replace("\n", "\\n")
            if trusted:
                trusted = trusted.strip().replace("\n", "\\n")
            if cert:
                if not cert.startswith("-----BEGIN") or "CERTIFICATE" not in cert:
                    raise ValueError("Certificate chain must be in PEM format")
                env("KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN", cert)
            if key:
                if not key.startswith("-----BEGIN") or "PRIVATE KEY" not in key:
                    raise ValueError("Private key must be in PEM format")
                env("KAFKA_SSL_KEYSTORE_KEY", key)
            if cert or key:
                env("KAFKA_SSL_KEYSTORE_TYPE", "PEM")
            if key and password:
                env("KAFKA_SSL_KEY_PASSWORD", password)
            if trusted:
                if not trusted.startswith("-----BEGIN") or "CERTIFICATE" not in trusted:
                    raise ValueError("Trusted certificates must be in PEM format")
                env("KAFKA_SSL_TRUSTSTORE_CERTIFICATES", trusted)
                env("KAFKA_SSL_TRUSTSTORE_TYPE", "PEM")
            if client_auth:
                env("KAFKA_SSL_CLIENT_AUTH", client_auth)
            env("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", " ")

    def tc_start(self) -> None:
        """Start the test container."""
        protocol = self.protocol
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        listeners = f"{protocol}://{host}:{port},BROKER://127.0.0.1:{self.broker_port}"
        data = (
            dedent(
                f"""
                #!/bin/bash
                {self.boot_command}
                export KAFKA_ADVERTISED_LISTENERS={listeners}
                c=/etc/confluent/docker
                . $c/bash-config
                # workaround for https://github.com/confluentinc/kafka-images/issues/244
                sed -i -E '/^if .*LISTENERS.*SSL:/,/^fi/d' $c/configure
                $c/configure
                $c/launch
                """
            )
            .strip()
            .encode("utf-8")
        )
        self.create_file(data, KafkaContainer.TC_START_SCRIPT)

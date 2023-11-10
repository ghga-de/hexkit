# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

import tarfile
import time
from io import BytesIO
from ssl import SSLError
from textwrap import dedent
from typing import Literal, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, UnrecognizedBrokerVersion
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

__all__ = ["KafkaSSLContainer"]

DEFAULT_IMAGE = "confluentinc/cp-kafka:7.5.1"

DEFAULT_PORT = 9093  # default port for the Kafka container
BROKER_PORT = 9092  # auxiliary port for inter broker listener


class KafkaSSLContainer(DockerContainer):
    """Kafka container that supports SSL (or actually TLS)."""

    TC_START_SCRIPT = "/tc-start.sh"
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
        super().__init__(image, **kwargs)
        env = self.with_env
        self.port = port
        ssl = bool(cert or trusted or client_auth)
        protocol = "SSL" if ssl else "PLAINTEXT"
        self.protocol = protocol
        self.with_exposed_ports(port)
        self.broker_port = DEFAULT_PORT if port == BROKER_PORT else BROKER_PORT
        listeners = f"{protocol}://0.0.0.0:{port},BROKER://0.0.0.0:{self.broker_port}"
        protocol_map = f"BROKER:PLAINTEXT,{protocol}:{protocol}"
        env("KAFKA_LISTENERS", listeners)
        env("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
        env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", protocol_map)
        env("KAFKA_BROKER_ID", "1")
        env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        env("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
        env("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "10000000")
        env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
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

    def get_bootstrap_server(self) -> str:
        """Get the Kafka bootstrap server."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        return f"{host}:{port}"

    def start(self) -> "KafkaSSLContainer":
        """Start the Docker container."""
        script = self.TC_START_SCRIPT
        command = f'sh -c "while [ ! -f {script} ]; do sleep 0.1; done; sh {script}"'
        self.with_command(command)
        super().start()
        self.tc_start()
        self._connect()
        return self

    @wait_container_is_ready(
        UnrecognizedBrokerVersion, NoBrokersAvailable, KafkaError, ValueError
    )  # pyright: ignore
    def _connect(self) -> None:
        bootstrap_server = self.get_bootstrap_server()
        try:
            consumer = KafkaConsumer(
                group_id="test",
                bootstrap_servers=[bootstrap_server],
                security_protocol=self.protocol,
            )
        except SSLError:
            pass  # count this as connected
        else:
            if not consumer.bootstrap_connected():
                raise KafkaError("Unable to connect with Kafka container!")

    def tc_start(self) -> None:
        """Start the test container."""
        protocol = self.protocol
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port)
        listeners = f"{protocol}://{host}:{port},BROKER://127.0.0.1:{self.broker_port}"
        script = f"""
        #!/bin/bash
        c=/etc/confluent/docker
        . $c/bash-config
        export KAFKA_ADVERTISED_LISTENERS={listeners}
        export KAFKA_ZOOKEEPER_CONNECT=localhost:2181
        p=zookeeper.properties
        echo "clientPort=2181" > $p
        echo "dataDir=/var/lib/zookeeper/data" >> $p
        echo "dataLogDir=/var/lib/zookeeper/log" >> $p
        zookeeper-server-start $p &
        # workaround for https://github.com/confluentinc/kafka-images/issues/244
        sed -i -E '/^if .*LISTENERS.*SSL:/,/^fi/d' $c/configure
        $c/configure && $c/launch
        """
        self.create_file(dedent(script).strip().encode("utf-8"), self.TC_START_SCRIPT)

    def create_file(self, content: bytes, path: str) -> None:
        """Create a file inside the container."""
        with BytesIO() as archive:
            with tarfile.TarFile(fileobj=archive, mode="w") as tar:
                tarinfo = tarfile.TarInfo(name=path)
                tarinfo.size = len(content)
                tarinfo.mtime = time.time()  # type: ignore
                tar.addfile(tarinfo, BytesIO(content))
            archive.seek(0)
            self.get_wrapped_container().put_archive("/", archive)

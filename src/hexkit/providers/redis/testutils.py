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

"""Utilities for testing code that uses Redis providers.

Please note, only use for testing purposes.
"""

from collections.abc import Generator
from dataclasses import dataclass

import pytest
from pydantic import RedisDsn, Secret
from redis import Redis
from testcontainers.redis import RedisContainer

from hexkit.custom_types import PytestScope
from hexkit.providers.redis.config import RedisConfig

__all__ = [
    "REDIS_IMAGE",
    "RedisConfig",
    "RedisContainerFixture",
    "RedisFixture",
    "clean_redis_fixture",
    "get_clean_redis_fixture",
    "get_persistent_redis_fixture",
    "get_redis_container_fixture",
    "persistent_redis_fixture",
    "redis_container_fixture",
    "redis_fixture",
]

REDIS_IMAGE = "redis:8.4.0"


@dataclass(frozen=True)
class RedisFixture:
    """A fixture with utility methods for tests that use Redis."""

    client: Redis
    config: RedisConfig

    def flush_db(self):
        """Flush all keys from the current database."""
        self.client.flushdb()


class RedisContainerFixture(RedisContainer):
    """Redis test container with Redis configuration."""

    redis_config: RedisConfig


def _redis_container_fixture() -> Generator[RedisContainerFixture, None, None]:
    """Fixture function for getting a running Redis test container."""
    with RedisContainerFixture(image=REDIS_IMAGE) as redis_container:
        client = redis_container.get_client()
        conn_kwargs = client.connection_pool.connection_kwargs
        host = conn_kwargs["host"]
        port = conn_kwargs["port"]
        db = conn_kwargs.get("db", 0)
        redis_url = f"redis://{host}:{port}/{db}"
        redis_config = RedisConfig(redis_url=Secret(RedisDsn(redis_url)))
        redis_container.redis_config = redis_config
        yield redis_container


def get_redis_container_fixture(
    scope: PytestScope = "session", name: str = "redis_container"
):
    """Get a Redis test container fixture with desired scope and name.

    By default, the session scope is used for Redis test containers.
    """
    return pytest.fixture(_redis_container_fixture, scope=scope, name=name)


redis_container_fixture = get_redis_container_fixture()


def _persistent_redis_fixture(
    redis_container: RedisContainerFixture,
) -> Generator[RedisFixture, None, None]:
    """Fixture function that gets a persistent Redis fixture.

    The state of Redis is not cleaned up by the function.
    """
    config = redis_container.redis_config
    client = Redis.from_url(str(config.redis_url.get_secret_value()))
    try:
        yield RedisFixture(client=client, config=config)
    finally:
        client.close()


def get_persistent_redis_fixture(scope: PytestScope = "function", name: str = "redis"):
    """Get a Redis fixture with desired scope and name.

    The state of the Redis test container is persisted across tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Redis test container.
    """
    return pytest.fixture(_persistent_redis_fixture, scope=scope, name=name)


persistent_redis_fixture = get_persistent_redis_fixture()


def _clean_redis_fixture(
    redis_container: RedisContainerFixture,
) -> Generator[RedisFixture, None, None]:
    """Fixture function that gets a clean Redis fixture.

    The clean state is achieved by flushing the database upfront.
    """
    for redis_fixture in _persistent_redis_fixture(redis_container):
        redis_fixture.flush_db()
        yield redis_fixture


def get_clean_redis_fixture(scope: PytestScope = "function", name: str = "redis"):
    """Get a Redis fixture with desired scope and name.

    The state of Redis is reset by flushing the database before running tests.

    By default, the function scope is used for this fixture,
    while the session scope is used for the underlying Redis test container.
    """
    return pytest.fixture(_clean_redis_fixture, scope=scope, name=name)


redis_fixture = clean_redis_fixture = get_clean_redis_fixture()

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

"""Redis client configuration and connection management."""

from redis.asyncio import Redis

from hexkit.providers.redis.config import RedisConfig

__all__ = ["ConfiguredRedisClient"]


class ConfiguredRedisClient:
    """Context manager for Redis client with automatic connection lifecycle."""

    def __init__(self, config: RedisConfig):
        """Initialize with Redis configuration.

        Args:
            config: Redis-specific config parameters.
        """
        self._config = config
        self._client: Redis | None = None

    async def __aenter__(self) -> Redis:
        """Establish Redis connection."""
        # Convert Pydantic URL to string and extract secret value
        redis_url = str(self._config.redis_url.get_secret_value())

        # Create Redis client with URL and optional timeout
        self._client = Redis.from_url(
            redis_url,
            socket_timeout=self._config.redis_timeout,
            decode_responses=False,  # We handle encoding/decoding in the stores
        )
        return self._client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()

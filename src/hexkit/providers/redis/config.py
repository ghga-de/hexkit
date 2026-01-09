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

"""Redis-specific configuration."""

from pydantic import Field, PositiveInt, RedisDsn, Secret
from pydantic_settings import BaseSettings

__all__ = ["RedisConfig"]


class RedisConfig(BaseSettings):
    """Configuration parameters for connecting to a Redis server.

    Inherit your config class from this class if your application uses Redis.
    """

    redis_url: Secret[RedisDsn] = Field(
        ...,
        examples=["redis://localhost:6379/0", "redis://:password@localhost:6379/1"],
        description=(
            "Redis connection URL. Might include credentials and database selection."
            + " Format: redis://[[username]:password@]host[:port][/database]."
            + " For more information see:"
            + " https://redis.io/docs/connect/clients/"
        ),
    )
    redis_timeout: PositiveInt | None = Field(
        default=None,
        examples=[30, 60, None],
        description=(
            "Socket timeout in seconds for Redis operations. When the timeout"
            + " expires, a timeout exception is raised. If set to None, operations"
            + " will not time out (default Redis behavior)."
        ),
    )

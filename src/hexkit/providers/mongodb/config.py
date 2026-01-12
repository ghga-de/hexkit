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

"""MongoDB specific configuration."""

from pydantic import Field, MongoDsn, PositiveInt, Secret
from pydantic_settings import BaseSettings

__all__ = ["MongoDbConfig"]


class MongoDbConfig(BaseSettings):
    """Configuration parameters for connecting to a MongoDB server.

    Inherit your config class from this class if your application uses MongoDB.
    """

    mongo_dsn: Secret[MongoDsn] = Field(
        ...,
        examples=["mongodb://localhost:27017"],
        description=(
            "MongoDB connection string. Might include credentials."
            + " For more information see:"
            + " https://naiveskill.com/mongodb-connection-string/"
        ),
    )
    db_name: str = Field(
        ...,
        examples=["my-database"],
        description="Name of the database located on the MongoDB server.",
    )
    mongo_timeout: PositiveInt | None = Field(
        default=None,
        examples=[300, 600, None],
        description=(
            "Timeout in seconds for API calls to MongoDB. The timeout applies to all steps"
            + " needed to complete the operation, including server selection, connection"
            + " checkout, serialization, and server-side execution. When the timeout"
            + " expires, PyMongo raises a timeout exception. If set to None, the"
            + " operation will not time out (default MongoDB behavior)."
        ),
    )

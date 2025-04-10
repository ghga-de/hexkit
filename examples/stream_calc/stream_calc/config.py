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

"""Config parameters."""

from typing import Literal

from hexkit.config import config_from_yaml
from hexkit.providers.akafka import KafkaConfig
from stream_calc.translators.eventpub import EventResultEmitterConfig
from stream_calc.translators.eventsub import EventProblemReceiverConfig

LOGLEVEL = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@config_from_yaml(prefix="stream_calc")
class Config(KafkaConfig, EventProblemReceiverConfig, EventResultEmitterConfig):
    """Config parameters and their defaults."""

    # adding defaults to params defined in the KafkaConfig, just for convenience in this
    # example:
    service_name: str = "stream_calc"
    service_instance_id: str = "1"
    kafka_servers: list[str] = ["kafka:9092"]

    log_level: LOGLEVEL = "INFO"

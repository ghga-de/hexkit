# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

from pydantic import BaseSettings

LOGLEVEL = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class Config(BaseSettings):
    """Config parameters and their defaults."""

    service_name: str = "stream_calc"
    client_suffix: str = "1"
    kafka_servers: list[str] = ["kafka:9092"]
    log_level: LOGLEVEL = "INFO"
    result_emit_output_topic: str = "calc_output"
    result_emit_success_type: str = "calc_success"
    result_emit_failure_typte: str = "calc_failure"
    problem_receive_topics: list[str] = ["arithmetic_problems"]

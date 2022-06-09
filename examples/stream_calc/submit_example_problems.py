#!/usr/bin/env python3

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

"""A script for submitting example problems to the stream calculator app."""

from sc_tests.integration.test_event_api import (
    CASES,
    DEFAULT_CONFIG,
    await_problem_outcomes,
    submit_test_problems,
)

KAFKA_SERVER = DEFAULT_CONFIG.kafka_servers[0]

submit_test_problems(CASES, kafka_server=KAFKA_SERVER)
await_problem_outcomes(CASES, kafka_server=KAFKA_SERVER)
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

"""This script consumes events from a Kafka topic"""

# pylint: disable-all


import json
from pathlib import Path

from pyquail.akafka import EventConsumer, KafkaConfigBase

HERE = Path(__file__).parent.resolve()

TOPIC_NAME = "my_topic"
SERVICE_NAME = "consumer"
EVENT_TYPE = "counter"
EVENT_KEY = "count"
KAFKA_SERVER = "kafka:9092"

with open(HERE / "message_schema.json", "r") as schema_file:
    EVENT_SCHEMAS = {EVENT_TYPE: json.load(schema_file)}

EXEC_FUNCS = {EVENT_TYPE: print}

CONFIG = KafkaConfigBase(
    service_name=SERVICE_NAME, client_suffix="1", kafka_servers=[KAFKA_SERVER]
)


def run():
    """Runs subscribing process."""

    with EventConsumer(
        config=CONFIG,
        topic_names=[TOPIC_NAME],
        event_schemas=EVENT_SCHEMAS,
        exec_funcs=EXEC_FUNCS,
    ) as consumer:
        consumer.subscribe(run_forever=True)


if __name__ == "__main__":
    run()

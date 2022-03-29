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

"""This script publishes events to a Kafka topic"""

# pylint: disable-all


import json
from pathlib import Path

from pyquail.akafka import EventProducer, KafkaConfigBase

HERE = Path(__file__).parent.resolve()

TOPIC_NAME = "my_topic"
SERVICE_NAME = "publisher"
EVENT_TYPE = "counter"
EVENT_KEY = "count"
KAFKA_SERVER = "kafka:9092"

with open(HERE / "message_schema.json", "r") as schema_file:
    EVENT_SCHEMAS = {EVENT_TYPE: json.load(schema_file)}

CONFIG = KafkaConfigBase(
    service_name=SERVICE_NAME, client_suffix="1", kafka_servers=[KAFKA_SERVER]
)


def run():
    """Runs publishing process."""

    with EventProducer(
        config=CONFIG, topic_name=TOPIC_NAME, event_schemas=EVENT_SCHEMAS
    ) as producer:
        # publish 10 messages:
        for count in range(0, 10):
            event_payload = {"count": count}
            producer.publish(
                event_type=EVENT_TYPE, event_key=EVENT_KEY, event_payload=event_payload
            )


if __name__ == "__main__":
    run()

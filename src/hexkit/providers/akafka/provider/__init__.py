# Copyright 2021 - 2024 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""A subpackage containing all Apache Kafka-specific providers.

Require dependencies of the `akafka` extra.
"""

from .daosub import KafkaOutboxSubscriber
from .eventpub import KafkaEventPublisher
from .eventsub import (
    ConsumerEvent,
    KafkaEventSubscriber,
    get_header_value,
    headers_as_dict,
)

__all__ = [
    "ConsumerEvent",
    "KafkaEventPublisher",
    "KafkaEventSubscriber",
    "KafkaOutboxSubscriber",
    "get_header_value",
    "headers_as_dict",
]

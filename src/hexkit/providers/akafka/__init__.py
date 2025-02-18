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

"""Apache Kafka-specific event publisher and subscriber provider.
They correspond to the `EventPublisherProtocol` and `EventSubscriberProtocol`,
respectively.
"""

from .config import KafkaConfig
from .provider import (
    KafkaEventPublisher,
    KafkaEventSubscriber,
    KafkaOutboxSubscriber,
    TranslatorConverter,
)

__all__ = [
    "KafkaConfig",
    "KafkaEventPublisher",
    "KafkaEventSubscriber",
    "KafkaOutboxSubscriber",
    "TranslatorConverter",
]

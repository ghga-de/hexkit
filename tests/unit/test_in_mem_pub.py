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

"""Testing the in-memory publisher."""

import pytest
from black import nullcontext

from hexkit.providers.testing.in_mem_pub import InMemEventPublisher, InMemEventStore
from hexkit.utils import NonAsciiStrError


@pytest.mark.parametrize(
    "type_, key, topic, expected_headers, exception",
    [
        ("test_type", "test_key", "test_topic", [("type", b"test_type")], None),
        (
            "test_ßtype",  # non ascii
            "test_key",
            "test_topic",
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_ßkey",  # non ascii
            "test_topic",
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
        (
            "test_type",
            "test_key",
            "test_ßtopic",  # non ascii
            [("type", b"test_type")],
            NonAsciiStrError,
        ),
    ],
)
def test_in_mem_publisher(type_, key, topic, expected_headers, exception):
    """Test the InMemEventPublisher testing utilities."""
    payload = {"test_content": "Hello World"}

    # create an in memory event store:
    event_store = InMemEventStore()

    # create event publisher:
    event_publisher = InMemEventPublisher(event_store=event_store)

    # publish event using the provider:
    with (pytest.raises(exception) if exception else nullcontext()):
        event_publisher.publish(
            payload=payload,
            type_=type_,
            key=key,
            topic=topic,
        )

    if not exception:
        # check if producer was correctly used:
        stored_event = event_store.get(topic)
        assert stored_event.key == key
        assert stored_event.payload == payload
        assert stored_event.type_ == type_

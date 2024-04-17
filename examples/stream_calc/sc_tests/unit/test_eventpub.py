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

"""Testing the `translators.eventpub` module."""

import pytest

from hexkit.providers.testing.eventpub import InMemEventPublisher
from stream_calc.translators.eventpub import (
    EventResultEmitter,
    EventResultEmitterConfig,
)


@pytest.mark.asyncio
async def test_emit_result():
    """Test the `emit_result` method of the EventResultEmitter using an in-memory
    event pulisher."""
    problem_id = "some_problem"
    result = 2.5
    output_topic = "test_out"
    success_type = "test_success"
    failure_type = "test_fail"
    event_publisher = InMemEventPublisher()

    config = EventResultEmitterConfig(
        result_emit_output_topic=output_topic,
        result_emit_success_type=success_type,
        result_emit_failure_type=failure_type,
    )
    result_emitter = EventResultEmitter(
        config=config,
        event_publisher=event_publisher,
    )
    await result_emitter.emit_result(problem_id=problem_id, result=result)

    # check the published event:
    event = event_publisher.event_store.get(topic=output_topic)
    assert event.payload == {"problem_id": problem_id, "result": result}
    assert event.key == problem_id
    assert event.type_ == success_type


@pytest.mark.asyncio
async def test_emit_failure():
    """Test the `emit_failure` method of the EventResultEmitter using an in-memory
    event pulisher."""
    problem_id = "some_problem"
    reason = "test reason"
    output_topic = "test_out"
    success_type = "test_success"
    failure_type = "test_fail"
    event_publisher = InMemEventPublisher()

    config = EventResultEmitterConfig(
        result_emit_output_topic=output_topic,
        result_emit_success_type=success_type,
        result_emit_failure_type=failure_type,
    )
    result_emitter = EventResultEmitter(
        config=config,
        event_publisher=event_publisher,
    )
    await result_emitter.emit_failure(problem_id=problem_id, reason=reason)

    # check the published event:
    event = event_publisher.event_store.get(topic=output_topic)
    assert event.payload == {"problem_id": problem_id, "reason": reason}
    assert event.key == problem_id
    assert event.type_ == failure_type

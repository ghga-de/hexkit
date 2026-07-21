# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Tests for the in-memory span capturing test harness from `hexkit.opentelemetry.testutils`."""

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util._once import Once

from hexkit.opentelemetry import OpenTelemetryConfig, configure_opentelemetry
from hexkit.opentelemetry.testutils import (
    _otel_fixture,
    _otel_provider_fixture,
    otel_fixture,  # noqa: F401
    otel_provider_fixture,  # noqa: F401
)

SERVICE_NAME = "test-service"


@pytest.fixture
def reset_global_tracer_provider(monkeypatch: pytest.MonkeyPatch):
    """Give a test a clean slate to configure its own global TracerProvider.

    OpenTelemetry only allows `set_tracer_provider()` to succeed once per process (an
    internal `Once` guards it), so tests that configure a TracerProvider themselves
    need to reset that guard first, and restore it afterward, to stay isolated from
    each other and from the session-scoped `otel_provider` fixture.
    """
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", None)
    monkeypatch.setattr(trace, "_TRACER_PROVIDER_SET_ONCE", Once())


def emit_span(name: str) -> None:
    """Start and immediately end a span with the given name on the global tracer."""
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(name):
        pass


def test_captures_emitted_spans(otel):
    """Ensure spans emitted while the fixture is active show up immediately.

    Also implicitly checks that no span leaked in from a previous test.
    """
    assert otel.get_span_names() == []

    emit_span("do-thing")

    otel.assert_has_span("do-thing")
    assert otel.get_span_names() == ["do-thing"]


def test_span_order_is_preserved(otel):
    """Ensure spans are returned in the order they finished, not just any order."""
    assert otel.get_span_names() == []

    emit_span("first")
    emit_span("second")
    emit_span("third")

    assert otel.get_span_names() == ["first", "second", "third"]


def test_assert_has_span_raises_with_useful_message(otel):
    """Ensure a missing span fails with an assertion listing what was captured."""
    emit_span("known-span")

    with pytest.raises(AssertionError) as exc_info:
        otel.assert_has_span("unknown-span")

    assert "unknown-span" in str(exc_info.value)
    assert "known-span" in str(exc_info.value)


def test_reset_clears_captured_spans(otel):
    """Ensure `reset()` drops all previously captured spans."""
    emit_span("to-be-cleared")
    assert otel.get_span_names() == ["to-be-cleared"]

    otel.reset()

    assert otel.get_span_names() == []


def test_standalone_provider_uses_in_memory_exporter(reset_global_tracer_provider):
    """Ensure that, when nothing has configured OpenTelemetry yet, the fixture
    configures its own TracerProvider wired to the in-memory exporter, never the real
    OTLP exporter.
    """
    assert not isinstance(trace.get_tracer_provider(), TracerProvider)

    provider_gen = _otel_provider_fixture(service_name=SERVICE_NAME)
    fixture = next(provider_gen)

    assert isinstance(trace.get_tracer_provider(), TracerProvider)

    emit_span("standalone-span")
    fixture.assert_has_span("standalone-span")

    provider_gen.close()


def test_attaches_to_already_configured_provider(reset_global_tracer_provider):
    """Ensure that, if OpenTelemetry has already been configured, the fixture attaches
    an in-memory exporter to that existing TracerProvider instead of replacing it -
    OpenTelemetry only allows the global TracerProvider to be set once.
    """
    own_exporter = InMemorySpanExporter()
    configure_opentelemetry(
        service_name=SERVICE_NAME,
        config=OpenTelemetryConfig(enable_opentelemetry=True),
        span_processor=SimpleSpanProcessor(own_exporter),
    )
    existing_provider = trace.get_tracer_provider()

    provider_gen = _otel_provider_fixture(service_name=SERVICE_NAME)
    fixture = next(provider_gen)

    # the provider was attached to, not replaced
    assert trace.get_tracer_provider() is existing_provider

    emit_span("shared-span")

    # both the app's own exporter and the fixture's in-memory exporter see the span
    fixture.assert_has_span("shared-span")
    assert [span.name for span in own_exporter.get_finished_spans()] == ["shared-span"]

    provider_gen.close()


def test_function_scoped_fixture_resets_before_and_after(reset_global_tracer_provider):
    """Ensure the function-scoped fixture hands back a clean slate on entry and cleans
    up the spans captured during the test on exit.
    """
    provider_gen = _otel_provider_fixture(service_name=SERVICE_NAME)
    provider_fixture = next(provider_gen)

    emit_span("leftover-from-before")
    assert provider_fixture.get_span_names() == ["leftover-from-before"]

    test_gen = _otel_fixture(provider_fixture)
    fixture = next(test_gen)
    assert fixture.get_span_names() == []  # reset on entry

    emit_span("during-test")
    assert fixture.get_span_names() == ["during-test"]

    with pytest.raises(StopIteration):
        next(test_gen)  # drive the generator past its final `reset()` on exit
    assert provider_fixture.get_span_names() == []  # reset on exit

    provider_gen.close()

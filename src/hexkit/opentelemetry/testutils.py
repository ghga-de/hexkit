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

"""Utilities for testing code that relies on OpenTelemetry instrumentation.

Please note, only use for testing purposes.
"""

from collections.abc import Generator
from dataclasses import dataclass

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from hexkit.custom_types import PytestScope
from hexkit.opentelemetry import OpenTelemetryConfig, configure_opentelemetry

__all__ = [
    "OpenTelemetryFixture",
    "get_otel_fixture",
    "get_otel_provider_fixture",
    "otel_fixture",
    "otel_provider_fixture",
]


@dataclass(frozen=True)
class OpenTelemetryFixture:
    """A fixture with utility methods for tests that verify emitted spans."""

    exporter: InMemorySpanExporter

    def reset(self) -> None:
        """Clear all previously captured spans."""
        self.exporter.clear()

    def get_finished_spans(self) -> tuple[ReadableSpan, ...]:
        """Return all spans finished so far, in the order they completed."""
        return tuple(self.exporter.get_finished_spans())

    def get_span_names(self) -> list[str]:
        """Return the names of all finished spans, in the order they completed."""
        return [span.name for span in self.get_finished_spans()]

    def assert_has_span(self, name: str) -> ReadableSpan:
        """Assert that a finished span with the given name exists and return it."""
        for span in self.get_finished_spans():
            if span.name == name:
                return span
        raise AssertionError(
            f"No span named {name!r} found. Captured spans: {self.get_span_names()}"
        )


def _otel_provider_fixture(
    service_name: str = "test",
) -> Generator[OpenTelemetryFixture, None, None]:
    """Fixture function attaching an in-memory span exporter for the test session.

    If OpenTelemetry has already been configured (e.g. because the app under test
    calls `configure_opentelemetry()` on startup), this attaches to the resulting
    TracerProvider so the exact same setup (sampler, resource, instrumented
    libraries) is exercised. Otherwise, it configures one itself via
    `configure_opentelemetry()` so the fixture also works standalone.

    A `SimpleSpanProcessor` is used instead of the `BatchSpanProcessor` used for real
    exports, so spans show up as soon as they end instead of on a batching interval.
    """
    provider = trace.get_tracer_provider()
    exporter = InMemorySpanExporter()

    if not isinstance(provider, TracerProvider):
        # No app under test has configured OpenTelemetry yet, so do it here, wiring
        # up the in-memory exporter directly instead of the default OTLP exporter -
        # this avoids pointless connection attempts against a collector in tests.
        configure_opentelemetry(
            service_name=service_name,
            config=OpenTelemetryConfig(enable_opentelemetry=True),
            span_processor=SimpleSpanProcessor(exporter),
        )
    else:
        # OpenTelemetry has already been configured (e.g. by the app under test),
        # so attach to its existing TracerProvider instead of replacing it -
        # OpenTelemetry only allows the global TracerProvider to be set once.
        # Detaching after the test is not possible, as there's no stable public API for that
        provider.add_span_processor(SimpleSpanProcessor(exporter))

    yield OpenTelemetryFixture(exporter=exporter)


def get_otel_provider_fixture(
    scope: PytestScope = "session", name: str = "otel_provider"
):
    """Get a fixture with desired scope and name that sets up span capturing.

    By default, the session scope is used since only one `TracerProvider` can be
    configured per process.
    """
    return pytest.fixture(_otel_provider_fixture, scope=scope, name=name)


otel_provider_fixture = get_otel_provider_fixture()


def _otel_fixture(
    otel_provider: OpenTelemetryFixture,
) -> Generator[OpenTelemetryFixture, None, None]:
    """Fixture function that gets a clean slate of captured spans for a single test."""
    otel_provider.reset()
    yield otel_provider
    otel_provider.reset()


def get_otel_fixture(scope: PytestScope = "function", name: str = "otel"):
    """Get a fixture with desired scope and name for asserting on emitted spans.

    By default, the function scope is used so captured spans don't leak between
    tests, while the session scope is used for the underlying span capturing setup.
    """
    return pytest.fixture(_otel_fixture, scope=scope, name=name)


otel_fixture = get_otel_fixture()

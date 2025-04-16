# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""OpenTelemetry specific configuration code. This is gated behind the opentelemetry extra."""

import os
from typing import Callable

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.environment_variables import OTEL_EXPORTER_OTLP_PROTOCOL
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def configure_tracer(service_name: str, protocol: str = "http/protobuf"):
    """Set up a global tracer for a specific service using the given exporter protocol."""
    # opentelemetry distro sets this to grpc, but in the current context http/protobuf is preferred
    os.environ.setdefault(OTEL_EXPORTER_OTLP_PROTOCOL, protocol)

    resource = Resource(attributes={SERVICE_NAME: service_name})
    trace_provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter())
    trace_provider.add_span_processor(processor)
    trace.set_tracer_provider(trace_provider)


class SpanTracer:
    """Custom tracer class providing a decorator to autpopulate span names."""

    def __init__(self, name):
        self.tracer = trace.get_tracer(name)

    def start_span(
        self, *, record_exception: bool = False, set_status_on_exception: bool = False
    ):
        """Decorator function starting a span populated with the __qualname__ of the wrapped function"""

        def outer_wrapper(function: Callable):
            def traced_function(*args, **kwargs):
                name = function.__qualname__
                with self.tracer.start_as_current_span(
                    name,
                    record_exception=record_exception,
                    set_status_on_exception=set_status_on_exception,
                ):
                    return function(*args, **kwargs)

            return traced_function

        return outer_wrapper

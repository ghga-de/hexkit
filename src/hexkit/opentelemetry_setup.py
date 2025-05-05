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

import logging
import os
from typing import Callable, Literal, Optional

from opentelemetry import trace
from opentelemetry.environment_variables import (
    OTEL_LOGS_EXPORTER,
    OTEL_METRICS_EXPORTER,
    OTEL_TRACES_EXPORTER,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.environment_variables import (
    OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_SDK_DISABLED,
)
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio

TRACER: Optional["SpanTracer"] = None

logger = logging.getLogger(__name__)


def configure_tracer(
    *,
    service_name: str,
    enable_otel: bool = False,
    protocol: Literal["grpc", "http/protobuf"] = "http/protobuf",
    sampling_rate: float = 1,
):
    """Set up a global tracer for a specific service using the given exporter protocol."""
    global TRACER
    # opentelemetry distro sets this to grpc, but in the current context http/protobuf is preferred
    os.environ.setdefault(OTEL_EXPORTER_OTLP_PROTOCOL, protocol)
    # Disable OpenTelemetry metrics and logs explicitly as they are not processed in the backend currently
    # This overwrites the defaults of `otlp` set in opentelemetry distro
    os.environ.setdefault(OTEL_METRICS_EXPORTER, "none")
    os.environ.setdefault(OTEL_LOGS_EXPORTER, "none")

    if enable_otel:
        if TRACER is not None:
            logger.warning(
                "OpenTelemetry configuration code should only be run once. "
                "If it has been run with a different service name than %s before, "
                "the tracer and resource name will likely be wrong in some cases.",
                service_name,
            )
        resource = Resource(attributes={SERVICE_NAME: service_name})
        # Replace the default static sampler with a probabilistic one
        # With the default sampling rate, behaviour does not change, but this allows to
        # introduce head sampling by adjusting a config option on the service side later on
        if sampling_rate > 1:
            sampling_rate = 1
            logger.info(
                "Provided sampling rate has to be between 0 and 1. Adjusted from %i to 1.",
                sampling_rate,
            )
        elif sampling_rate < 0:
            sampling_rate = 0
            logger.info(
                "Provided sampling rate has to be between 0 and 1. Adjusted from %i to 0.",
                sampling_rate,
            )
        # Initiate a sampler that honors parent span sampling decisions
        # This should consistently yield full traces within a service but not necessarily
        # across service boundaries
        sampler = ParentBasedTraceIdRatio(rate=sampling_rate)

        # Initialize service specific TracerProvider
        trace_provider = TracerProvider(resource=resource, sampler=sampler)
        processor = BatchSpanProcessor(OTLPSpanExporter())
        trace_provider.add_span_processor(processor)
        trace.set_tracer_provider(trace_provider)
        TRACER = SpanTracer(service_name)
    else:
        # Currently OTEL_SDK_DISABLED doesn't seem to be honored by all implementations yet
        # It seems to be working well enough for the Python implementation, but to be on
        # the safe side, let's explicitly disable the trace exporter for now
        os.environ.setdefault(OTEL_TRACES_EXPORTER, "none")
        os.environ.setdefault(OTEL_SDK_DISABLED, "true")


def start_span(
    *,
    record_exception: bool = False,
    set_status_on_exception: bool = False,
) -> Callable:
    """Returns decorated or undecorated function depending on if TRACER is instantiated.

    Should be used as a decorator.
    """

    def wrapper(function: Callable):
        # Caller did not have any time to populate from config yet or otel is disabled
        if TRACER is None:
            return function
        # Return decorated function
        return TRACER.start_span(
            record_exception=record_exception,
            set_status_on_exception=set_status_on_exception,
        )(function)

    return wrapper


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

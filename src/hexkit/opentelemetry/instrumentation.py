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

"""OpenTelemetry specific configuration code. This is gated behind the opentelemetry extra."""

import importlib
import logging
from typing import Annotated

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio
from pydantic import Field
from pydantic_settings import BaseSettings

__all__ = [
    "OpenTelemetryConfig",
    "configure_opentelemetry",
    "instrument_installed_libraries",
]

logger = logging.getLogger(__name__)

# Instrumentation modules corresponding to the `opentelemetry-*` optional extras.
# Each entry is only instrumented if its module is installed.
_INSTRUMENTOR_MODULES: tuple[tuple[str, str], ...] = (
    ("opentelemetry.instrumentation.httpx", "HTTPXClientInstrumentor"),
    ("opentelemetry.instrumentation.aiokafka", "AIOKafkaInstrumentor"),
    ("opentelemetry.instrumentation.pymongo", "PymongoInstrumentor"),
    ("opentelemetry.instrumentation.fastapi", "FastAPIInstrumentor"),
    ("opentelemetry.instrumentation.botocore", "BotocoreInstrumentor"),
    ("opentelemetry.instrumentation.redis", "RedisInstrumentor"),
)


def instrument_installed_libraries() -> None:
    """Run `.instrument()` for every instrumentation library that is installed."""
    for module_name, class_name in _INSTRUMENTOR_MODULES:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            logger.debug("'%s' not installed, skipping instrumentation.", module_name)
            continue
        try:
            instrumentor_class = getattr(module, class_name)
        except AttributeError:
            logger.warning(
                "Could not instrument '%s' for '%s'.", class_name, module_name
            )
        instrumentor_class().instrument()
        logger.info("Instrumented '%s' for '%s'.", class_name, module_name)


class OpenTelemetryConfig(BaseSettings):
    """OpenTelemetry specific configuration options"""

    enable_opentelemetry: bool = Field(
        default=False,
        description="If set to true, this will run necessary setup code."
        "If set to false, no setup code is run, which leaves tracing disabled.",
    )
    otel_trace_sampling_rate: Annotated[float, Field(strict=True, ge=0, le=1)] = Field(
        default=1.0,
        description="Determines which proportion of spans should be sampled. "
        "A value of 1.0 means all and is equivalent to the previous behaviour. "
        "Setting this to 0 will result in no spans being sampled, but this does not "
        "automatically set `enable_opentelemetry` to False.",
    )


def configure_opentelemetry(
    *,
    service_name: str,
    config: OpenTelemetryConfig,
    span_processor: SpanProcessor | None = None,
):
    """Configure all needed parts of OpenTelemetry.

    This needs to be called before constructing any objects that are instrumented, e.g.
    construction a FastAPI app before running this, will result in missing instrumentation.

    Setup of the TracerProvider is done programmatically. If disabled, this is a no-op,
    leaving the default no-op TracerProvider from the OpenTelemetry API in place.

    By default, spans are exported via OTLP over HTTP using a `BatchSpanProcessor`.
    """
    if not config.enable_opentelemetry:
        logger.info("OpenTelemetry disabled via config.")
        return

    resource = Resource(attributes={SERVICE_NAME: service_name})
    # Replace the default static sampler with a probabilistic one that honors parent
    # span sampling decisions
    # This should consistently yield full traces within a service but not necessarily
    # across service boundaries
    # With the default sampling rate, behaviour does not change, but this allows to
    # introduce head sampling by adjusting a config option on the service side later on
    sampler = ParentBasedTraceIdRatio(rate=config.otel_trace_sampling_rate)

    # Initialize service specific TracerProvider
    trace_provider = TracerProvider(resource=resource, sampler=sampler)
    trace_provider.add_span_processor(
        span_processor or BatchSpanProcessor(OTLPSpanExporter())
    )
    trace.set_tracer_provider(trace_provider)

    instrument_installed_libraries()

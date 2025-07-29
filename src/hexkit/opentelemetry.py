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
from typing import Annotated

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.environment_variables import OTEL_SDK_DISABLED
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio
from pydantic import Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class OpenTelemetryConfig(BaseSettings):
    """OpenTelemetry specific configuration options"""

    enable_opentelemetry: bool = Field(
        default=False,
        description="If set to true, this will run necessary setup code."
        "If set to false, environment variables are set that should also effectively "
        "disable autoinstrumentation.",
    )
    otel_trace_sampling_rate: Annotated[float, Field(strict=True, ge=0, le=1)] = Field(
        default=1.0,
        description="Determines which proportion of spans should be sampled. "
        "A value of 1.0 means all and is equivalent to the previous behaviour. "
        "Setting this to 0 will result in no spans being sampled, but this does not "
        "automatically set `enable_opentelemetry` to False.",
    )


def configure_opentelemetry(*, service_name: str, config: OpenTelemetryConfig):
    """Configure all needed parts of OpenTelemetry.

    Setup of the TracerProvider is done programmatically, while disabling OpenTelemetry
    sets the corresponding environment variable.
    """
    if config.enable_opentelemetry:
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
        processor = BatchSpanProcessor(OTLPSpanExporter())
        trace_provider.add_span_processor(processor)
        trace.set_tracer_provider(trace_provider)
    else:
        # Currently OTEL_SDK_DISABLED doesn't seem to be honored by all implementations yet
        # It seems to be working well enough for the Python implementation.
        os.environ[OTEL_SDK_DISABLED] = "true"

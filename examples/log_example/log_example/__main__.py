# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Main entrypoint of the package."""

import asyncio
import logging

import log_example
from hexkit.log import LoggingConfig, configure_logging
from log_example.some_module import some_function

app_logger = logging.getLogger(log_example.__name__)


def main():
    config = LoggingConfig(service_name="log_example", service_instance_id="001")
    configure_logging(logger=app_logger, config=config)

    asyncio.run(some_function())


if __name__ == "__main__":
    main()

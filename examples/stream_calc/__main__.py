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

"""Main entry point of the package."""

from stream_calc.config import Config
from stream_calc.container import Container

if __name__ == "__main__":
    config = Config()
    container = Container()
    container.config.from_pydantic(config)
    container.init_resources()

    calculator = container.stream_calculator()
    calculator.multiply(task_id="test-1", multiplier=2, multiplicand=2)
    calculator.devide(task_id="test-2", dividend=2, divisor=2)
    calculator.devide(task_id="test-3", dividend=2, divisor=0)

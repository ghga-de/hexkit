#!/usr/bin/env python3

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

"""Run all update scripts that are present in the repository in the correct order"""

try:
    from scripts.update_template_files import main as update_template
except ImportError:
    pass
else:
    print("Pulling in updates from template repository")
    update_template()

try:
    from scripts.update_config_docs import main as update_config
except ImportError:
    pass
else:
    print("Updating config docs")
    update_config()

try:
    from scripts.update_openapi_docs import main as update_openapi
except ImportError:
    pass
else:
    print("Updating OpenAPI docs")
    update_openapi()

try:
    from scripts.update_readme import main as update_readme
except ImportError:
    pass
else:
    print("Updating README")
    update_readme()

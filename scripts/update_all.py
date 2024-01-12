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
    from update_template_files import main as update_template
except ImportError:
    print("update_template_files script not found")
else:
    print("Pulling in updates from template repository")
    update_template()

try:
    from update_pyproject import main as update_pyproject
except ImportError:
    print("update_pyproject script not found")
else:
    print("Updating pyproject.toml file")
    update_pyproject()

try:
    from update_lock import main as update_lock
except ImportError:
    print("update_lock script not found")
else:
    print("Upgrading the lock file")
    update_lock(upgrade=True)

try:
    from update_hook_revs import main as update_hook_revs
except ImportError:
    print("update_hook_revs script not found")
else:
    print("Updating config docs")
    update_hook_revs()

try:
    from update_config_docs import main as update_config
except ImportError:
    print("update_config_docs script not found")
else:
    print("Updating config docs")
    update_config()

try:
    from update_openapi_docs import main as update_openapi
except ImportError:
    print("update_openapi_docs script not found")
else:
    print("Updating OpenAPI docs")
    update_openapi()

try:
    from update_readme import main as update_readme
except ImportError:
    print("update_readme script not found")
else:
    print("Updating README")
    update_readme()

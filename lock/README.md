<!--
 Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
 for the German Human Genome-Phenome Archive (GHGA)

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

-->

# Lock Files

This directory contains two lock files locking the dependencies of this microservice:

The [`./requirements.txt`](./requirements.txt) contains production dependencies.

The [`./requirements-dev.txt`](./requirements-dev.txt) additionally contains development
dependencies.

## Sources

For generating the production lock file, only the dependencies specified in the
[`../pyproject.toml`](../pyproject.toml) are considered as input.

For generating the development lock file, additionally, the
[`./requirements-dev-template.in`](./requirements-dev-template.in) as well as
the [`./requirements-dev.in`](./requirements-dev.in) are considered.

The `./requirements-dev-template.in` is automatically updated from the template
repository and should not be manually modified.

If you require additional dev dependencies not part of the
`./requirements-dev-template.in`, you can add them to the
`./requirements-dev.in`.

## Update and Upgrade

The lock files can be updated running the
[`../scripts/update_lock.py`](../scripts/update_lock.py) script. This will keep the
dependency versions in the lockfile unchanged unless they are in conflict with the
the input sources. In that case, the affected dependencies are updated to the latest
versions compatible with the input.

If you would like to upgrade all dependencies in the lock file to the latest versions
compatible with the input, you can run `../scripts/update_lock.py --upgrade`.

If you just want to check if the script would do update, you can run
`../scripts/update_lock.py --check`.

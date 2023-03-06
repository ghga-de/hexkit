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

# When mypy is started from pre-commit, it exits with an exception when analysing this
# file without useful error message, thus ignoring.
# (Please note this does not occur when running mypy directly.)
#  type: ignore

"""Module hosting the dependency injection container."""

from auth_demo.auth.config import AuthContext
from auth_demo.config import Config
from auth_demo.core import Hangout

from hexkit.inject import ContainerBase, get_configurator, get_constructor
from hexkit.providers.jwtauth import JWTAuthContextProvider

__all__ = ["Container"]


class Container(ContainerBase):
    """DI Container"""

    config = get_configurator(Config)

    hangout = get_constructor(Hangout, config=config)

    auth_provider = get_constructor(
        JWTAuthContextProvider, config=config, context_class=AuthContext
    )

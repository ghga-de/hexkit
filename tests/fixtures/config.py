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

"""Config fixtures"""

import copy
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel, BaseSettings

from hexkit.config import DEFAULT_CONFIG_PREFIX

from . import BASE_DIR, utils

# read all config yamls:
CONFIG_YAML_PATTERN = r"(.*)\.yaml"
CONFIG_YAML_DIR = BASE_DIR / "config_yamls"


class ConfigYamlFixture(BaseModel):
    """Container for config yaml fixtures"""

    path: Path
    content: Dict[str, Any]


def read_config_yaml(name: str):
    """Read config yaml."""
    path = CONFIG_YAML_DIR / name
    content = utils.read_yaml(path)

    return ConfigYamlFixture(path=path, content=content)


config_yamls = {
    re.match(CONFIG_YAML_PATTERN, cfile).group(1): read_config_yaml(cfile)  # type: ignore
    for cfile in os.listdir(CONFIG_YAML_DIR)
    if re.match(CONFIG_YAML_PATTERN, cfile)
}


# read env variable sets:
@dataclass
class EnvVarFixture:
    """Container for env var set. This class can be used
    as context manager so that the env vars are available
    within a with block but, after leaving the with block,
    the original enviroment is restored."""

    env_vars: Dict[str, str]
    prefix: str = DEFAULT_CONFIG_PREFIX

    def __enter__(self):
        """makes a backup of the environment and set the
        env_vars"""
        # pylint: disable=attribute-defined-outside-init
        self.env_backup = copy.deepcopy(os.environ)

        for name, value in self.env_vars.items():
            os.environ[f"{self.prefix}_{name}"] = value

    def __exit__(self, exc_type, exc_val, exc_tb):
        """restores the original environment"""
        os.environ = self.env_backup


def read_env_var_sets() -> Dict[str, EnvVarFixture]:
    """Read env vars sets and return a list of EnvVarFixtures."""
    env_var_dict = utils.read_yaml(BASE_DIR / "config_env_var_sets.yaml")

    return {
        set_name: EnvVarFixture(env_vars=env_vars)
        for set_name, env_vars in env_var_dict.items()
    }


env_var_sets = read_env_var_sets()


# pydantic BaseSettings classes:
class BasicConfig(BaseSettings):
    """Basic Config Example"""

    some_number: int
    some_string: str
    some_boolean: bool
    some_string_with_default: str = "default string"
    another_string_with_default: str = "another default string"

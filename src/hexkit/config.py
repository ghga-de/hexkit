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

"""Config parsing functionality based on pydantic's BaseSettings"""

import os
from pathlib import Path
from typing import Callable, Final, Optional

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

# Default config prefix:
DEFAULT_CONFIG_PREFIX: Final = "ghga_services"


class ConfigYamlDoesNotExist(RuntimeError):
    """Thrown when the context manager is used out of context."""

    def __init__(self, path: Path, specified_via: Optional[str] = None):
        message = (
            "The config yaml " + ""
            if specified_via is None
            else f"specified via {specified_via} " + f"does not exist: {path}"
        )
        super().__init__(message)


def get_default_config_yaml(prefix: str) -> Optional[Path]:
    """Get the path to the default config function.

    Args:
        prefix (str):
            Name prefix used to derive the default paths.
    """
    # check if path to config is set as env variable:
    path_env_var = f"{prefix.upper()}_CONFIG_YAML"
    try:
        path_via_env_var = Path(os.environ[path_env_var])
        if path_via_env_var.is_file():
            return path_via_env_var
        raise ConfigYamlDoesNotExist(
            path=path_via_env_var,
            specified_via=f"the environment variable {path_env_var}",
        )
    except KeyError:
        pass

    # construct file name from prefix:
    file_name = f".{prefix}.yaml"

    # look in the current directory:
    path_in_cwd = Path(os.getcwd()) / file_name
    if os.path.isfile(path_in_cwd):
        return path_in_cwd

    # look in the home directory:
    path_in_home = Path.home() / file_name
    if os.path.isfile(path_in_home):
        return path_in_home

    # if nothing was found return None:
    return None


def config_from_yaml(
    prefix: str = DEFAULT_CONFIG_PREFIX,
) -> Callable:
    """A factory that returns decorator functions which extends a
    pydantic BaseSettings class to read in parameters from a config yaml.
    It replaces (or adds) config settings to the BaseSettings class that configures
    the priorities for parameter sources as follows (highest priority first):
        - parameters passed using **kwargs
        - environment variables
        - file secrets
        - yaml config - path specified via env variable `{prefix}_CONFIG_YAML`
                        (all uppercase)
        - yaml config - file named `.{prefix}.yaml` and placed in the CWD or home dir
        - defaults

    Args:
        prefix: (str, optional):
            When defining parameters via environment variables, all variables
            have to be prefixed with this string following this pattern
            "{prefix}_{actual_variable_name}". Moreover, this prefix is used
            to derive the default location for the config yaml file
            ("~/.{prefix}.yaml"). Defaults to "ghga_services".
    """

    def decorator(settings) -> Callable:
        # settings should be a pydantic BaseSetting,
        # there is no type hint here to not restrict
        # autocompletion for attributes of the
        # modified settings class returned by the
        # contructor_wrapper
        """The actual decorator function.

        Args
            settings (BaseSettings):
                A pydantic BaseSettings class to be modified.
        """
        # check if settings inherits from pydantic BaseSettings:
        if not issubclass(settings, BaseSettings):
            raise TypeError(
                "The specified settings class is not a subclass of pydantic_settings.BaseSettings"
            )

        def constructor_wrapper(
            config_yaml: Optional[Path] = None,
            **kwargs,
        ):
            """A wrapper for constructing a pydantic BaseSetting with modified sources

            Args:
                config_yaml (str, optional):
                    Path to a config yaml. Overwrites the default location.
            """
            # get default path if config_yaml not specified:
            if config_yaml is None:
                config_yaml = get_default_config_yaml(prefix)
            elif not config_yaml.is_file():
                raise ConfigYamlDoesNotExist(path=config_yaml)

            class ModSettings(settings):
                """Modifies the original Settings class provided by the user"""

                model_config = SettingsConfigDict(frozen=True, env_prefix=f"{prefix}_")

                @classmethod
                def settings_customise_sources(
                    cls,
                    settings_cls: type[BaseSettings],
                    init_settings: PydanticBaseSettingsSource,
                    env_settings: PydanticBaseSettingsSource,
                    dotenv_settings: PydanticBaseSettingsSource,
                    file_secret_settings: PydanticBaseSettingsSource,
                ):
                    """Add custom yaml source, but maintain the priority of sources."""
                    return (
                        init_settings,
                        env_settings,
                        file_secret_settings,
                        YamlConfigSettingsSource(settings_cls, config_yaml),
                    )

            # construct settings class:
            return ModSettings(**kwargs)

        return constructor_wrapper

    return decorator

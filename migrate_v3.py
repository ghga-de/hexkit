#!/usr/bin/env python3

import configparser
import os
import re
from importlib import import_module

from scripts.get_package_name import get_package_name
from scripts.update_template_files import update_files


def get_version_from_init(filepath: str) -> str:
    """Pull `__version__` from the top `__init__.py`."""
    with open(filepath, encoding="utf-8") as file:
        for line in file:
            if line.startswith("__version__"):
                # Split the string on the equals sign and strip spaces and quotes
                return line.split("=")[1].strip().strip('"')
    raise RuntimeError("Unable to get version from __init__")


def get_config_from_setup(is_microservice: bool) -> dict[str, str]:
    """Get all applicable config from `setup.cfg`:

    1. metadata.description
    2. metadata.url
    3. options.install_requires
    4. options.extras_require
    5. options.entry_points.console_scripts
    """
    parser = configparser.ConfigParser()
    parser.read("./setup.cfg")
    metadata = parser["metadata"]
    options = parser["options"]
    extras_require = "options.extras_require"
    entrypoints = "options.entry_points"

    config = {}
    config["description"] = metadata["description"]
    config["url"] = metadata["url"]

    dependencies = options["install_requires"].strip()
    if is_microservice:
        dependencies = dependencies.replace("==", ">=")
    config["dependencies"] = [dependency for dependency in dependencies.split("\n")]

    if not is_microservice and extras_require in parser:
        extras = []
        for extra, deps in parser[extras_require].items():
            deps = [dep.replace("==", ">=") for dep in deps]

            if not deps:
                continue
            deps_formatted = [f'"{dep}"' for dep in deps]
            extra_formatted = f'{extra} = [{", ".join(deps_formatted)}]'
            extras.append(extra_formatted)
        if extras:
            config["optional-dependencies"] = extras

    if entrypoints in parser:
        config["scripts"] = {}
        scripts = parser[entrypoints]["console_scripts"].strip().split("\n")
        for script in scripts:
            name, value = script.split("=")
            config["scripts"][name.strip()] = value.strip()

    return config


def update_init(package_name: str, version: str):
    """Replace static version value in __init__.py with retrieval code."""
    with open(f"./src/{package_name}/__init__.py", encoding="utf-8") as init_read:
        content = init_read.read()

    content = re.sub(
        pattern=rf"__version__\s*=\s*\"{version}\"",
        repl="from importlib.metadata import version\n\n__version__ = version(__package__)",
        string=content,
    )
    with open(f"./src/{package_name}/__init__.py", "w", encoding="utf-8") as init_write:
        init_write.write(content)


def update_structure(package_name: str):
    """Move the code into `/src/`."""
    os.renames(f"./{package_name}", f"./src/{package_name}")


def pre_process_pyproject():
    """Remove comments from pyproject so tomli can write values okay"""
    with open("pyproject.toml", encoding="utf-8") as file:
        pre_processed = file.read()

    pre_processed = re.sub("# please adapt to package name", "", pre_processed)

    with open("pyproject.toml", "w", encoding="utf-8") as file:
        file.write(pre_processed)


def update_pyproject_toml(package_name: str, version: str, config: dict[str, str]):
    """Update the project metadata."""
    os.system("pip install tomli")
    tomli = import_module("tomli")
    with open("pyproject.toml", "rb") as pyproject_toml:
        pyproject = tomli.load(pyproject_toml)

    pyproject["project"]["name"] = package_name
    pyproject["project"]["version"] = version
    pyproject["project"]["description"] = config["description"]
    pyproject["project"]["dependencies"] = config["dependencies"]
    if "optional-dependencies" in config:
        pyproject["project"]["optional-dependencies"] = config["optional-dependencies"]
    pyproject["project"]["urls"] = {"Repository": config["url"]}

    # write the final output
    os.system("pip install tomli_w")
    tomli_w = import_module("tomli_w")
    with open("pyproject.toml", "wb") as modified_pyproject_toml:
        tomli_w.dump(pyproject, modified_pyproject_toml)


def collapse_list_string(match: re.Match) -> str:
    """Remove whitespace and trailing comma for one-element list"""
    i, j = match.span()
    substr = match.string[i:j]
    formatted = re.sub(r",\s*|\s*", "", substr)
    return formatted


def post_process_pyproject():
    """Collapse single-element lists"""
    with open("pyproject.toml", encoding="utf-8") as file:
        post_processed = file.read()

    arr_pattern = r"\[\s*(?:\".*?\",){1}\s*]"

    post_processed = re.sub(
        pattern=arr_pattern,
        repl=collapse_list_string,
        string=post_processed,
    )

    with open("pyproject.toml", "w", encoding="utf-8") as file:
        file.write(post_processed)


def update_template_files():
    """Run `scripts/update_template_files.py`."""
    os.system("scripts/update_template_files.py")


def prompt(msg: str):
    """Prompt user to do something before continuing."""
    while (
        input(f"{msg} (type 'done' to continue, CTRL+Z to quit)\n> ").strip().lower()
        != "done"
    ):
        pass


def delete(filename: str):
    """Remove the given file."""
    try:
        os.remove(filename)
    except:
        print(f"Skipped deleting {filename} because it doesn't exist")


def install_new_tools(package_name: str):
    """Install the packages needed to run the new scripts."""
    # grab the deps.py and lock_deps.py scripts because I forgot to update .static_files
    update_files(["scripts/script_utils/deps.py", "scripts/script_utils/lock_deps.py"])
    os.system("pip install httpx")
    os.system("pip install pip-tools")
    os.system("pip install stringcase")
    os.system(f"pip uninstall {package_name}")
    os.system("pip install -e .")


def ask_yes_no(msg: str) -> bool:
    """Ask user yes or no."""
    while (answer := input(f"{msg}\n> ").strip().lower()) not in ("y", "n"):
        pass

    return answer == "y"


def fix_requirements_dev():
    """Copy contents of `requirements-dev.txt` over to `.in` version

    Also make sure that dependencies are uncapped here.
    """
    with open("requirements-dev.txt", encoding="utf-8") as file:
        initial_content = file.read()

    updated_content = re.sub("common.txt", "common.in", initial_content)

    if initial_content == updated_content:
        updated_content += "\n-r requirements-dev-common.in"

    updated_content = re.sub("==", ">=", updated_content)

    with open("requirements-dev.txt", "w", encoding="utf-8") as file:
        file.write(updated_content)

    os.rename("./requirements-dev.txt", "./requirements-dev.in")


def main():
    """Apply all needed changes to bring the repo in line with latest template version."""
    is_microservice = ask_yes_no("Is this a microservice (i.e. not a library?) (y/n)")
    package_name = get_package_name()
    version = "0.11.0"

    update_structure(package_name)
    update_init(package_name, version)

    delete("./requirements.txt")
    delete("./requirements-dev.in")
    delete("./requirements-dev-common.txt")
    fix_requirements_dev()

    delete("./.pre-commit-config.yaml")
    update_files([".pre-commit-config.yaml"])

    install_new_tools(package_name)
    os.system("scripts/list_outdated_dependencies.py")
    prompt("Review the list above. Consider updating any outdated dependencies now.")

    print("\nBuilding lock files. This could take a few minutes...")
    os.system("scripts/update_lock.py")

    print("\nUpdating pre-commit-hook versions...")
    os.system("scripts/update_hook_revs.py")

    print("Done - rebuild the dev container")


if __name__ == "__main__":
    main()

![tests](https://github.com/ghga-de/pyquail/actions/workflows/unit_and_int_tests.yaml/badge.svg)
[![PyPI version shields.io](https://img.shields.io/pypi/v/pyquail.svg)](https://pypi.python.org/pypi/pyquail/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/pyquail.svg)](https://pypi.python.org/pypi/pyquail/)
[![codecov](https://codecov.io/gh/ghga-de/pyquail/branch/main/graph/badge.svg?token=V1VYRI4SEC)](https://codecov.io/gh/ghga-de/pyquail)

# pyquail
## Installation
This package is available at PyPI:
https://pypi.org/project/pyquail

You can install it from there using:
```
pip install pyquail
```

Thereby, you may specify following extra(s):
- `api`: dependencies needed to use the API server functionalities
- `dev`: dependencies needed for development and testing

## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode.

To use it, you have to have Docker as well as vscode with its "Remote - Containers" extension (`ms-vscode-remote.remote-containers`) extension installed.
Then, you just have to open this repo in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, there is follwing convenience command available
(please type it in the integrated terminal of vscode):
- `dev_install` - install the lib with all development dependencies and pre-commit hooks
(please run that if you are starting the devcontainer for the first time
or if added any python dependencies to the [`./setup.cfg`](./setup.cfg))

If you prefer not to use vscode, you could get a similar setup (without the editor specific features)
by running the following commands:
``` bash
# Execute in the repo's root dir:
cd ./.devcontainer

# build and run the environment with docker-compose
docker build -t pyquail .
docker run -it pyquail /bin/bash

```

## License
This repository is free to use and modify according to the [Apache 2.0 License](./LICENSE).

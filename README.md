![tests](https://github.com/ghga-de/hexkit/actions/workflows/tests.yaml/badge.svg)
[![PyPI version shields.io](https://img.shields.io/pypi/v/hexkit.svg)](https://pypi.python.org/pypi/hexkit/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/hexkit.svg)](https://pypi.python.org/pypi/hexkit/)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/hexkit/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/hexkit?branch=main)

# hexkit
A chassis library for building domain-focused, infrastructure-agnostic, and event-driven
microservices in Python.

## In a Nutshell
This project implements the Triple Hexagonal Architecture pattern which is an
optimization of the ordinary
[Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/) for
use with microservices.
In brief, adapters (as per the hexagonal architecture) are divided into two parts.
One part called *translator* is specific to one individual microservice and its
domain-oriented ports.
The other part called *provider* is service-agnostic but specific to one
technology of the surrounding infrastructure.
Both parts interact through a high-level interface called *protocol* (not to be
confused with Python's typing.Protocol) which is neither
specific to the technology nor to the microservice.
For more details on the Triple Hexagonal Architecture, refer to the concept paper that
can be found
[here](https://ghga-de.github.io/developer-cookbook/articles/2_triple_hexagonal/).

As a chassis lib, hexkit tries to reduce the redundancy and boilerplate across
microservices. It does so by providing ready-to-use triple-hexagonal building blocks
that are service-independent - hence protocols and providers. The only task that remains
for an individual service is to implement service-specific translators between the
service's ports and the general-purpose protocols (in addition to implementing the
domain functionality of the service, of course).

Hexkit is designed as a general-purpose library. However, it currently contains only
a limited collection of protocol-provider pairs that are of immediate interest to the
authors. We like to add support for more protocols and technologies over time.

The following protocols and providers are currently available:
| Protocol | Providers |
|---|---|
| Event Publishing | Apache Kafka |
| Event Subscription | Apache Kafka |
| Object Storage | S3(-compatible) |
| Data Access Object | MongoDB |

Hexkit does not force you to go all-in on the idea of Triple Hexagonal Architecture.
You can use it just for the technologies where you see benefits and use another approach
for the rest. For example, you could use hexkit for simplifying the exchange of events
between microservices but use a classical web framework such as FastAPI for designing
REST API and an ORM like SQLAlchemy for interacting with databases.

Feel free to develop hexagonal components yourself, whether or not you use the base
classes of hexkit. Not every protocol or provider has to be general-purpose, however,
if they are, please consider contributing them to hexkit.

## Getting started

Please have a look at the example application described in
[./examples/stream_calc](./examples/stream_calc).

We put a lot of effort into making the code self-documenting, so please do not be afraid
of jumping directly into it. You can find the protocols being defined at
[./src/hexkit/protocols](./src/hexkit/protocols) and the providers being implemented at
[./src/hexkit/providers](./src/hexkit/providers).

A more elaborate documentation and tutorial is on the way.

## Installation
This package is available at PyPI:
https://pypi.org/project/hexkit

You can install it from there using:
```
pip install hexkit
```

The following extras are available:
- `akafka`: when using the Apache Kafka-based event publishing or subscription
- `s3`: when interacting with S3-compatible Object Storages
- `mongodb`: when using mongodb as backend for the DAO protocol
- `test-akafka`: testing utils for Apache Kafka
- `test-s3`: testing utils for S3-compatible Object Storages
- `test-mongodb`: testing utils for MongoDB
- `all`: a union of all the above

## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode.

To use it, you need Docker and VS Code with the "Remote - Containers" extension
(`ms-vscode-remote.remote-containers`) installed.
Then, you just have to open this repo in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Additionally, the following convenience command is available inside the devcontainer
(type it in the integrated terminal of VS Code):
- `dev_install` - install the lib with all development dependencies and pre-commit hooks
(please run that if you are starting the devcontainer for the first time
or if added any python dependencies to the [`./setup.cfg`](./setup.cfg))

If you prefer not to use vscode, you could get a similar setup (without the editor
specific features) by running the following commands:
``` bash
# Execute in the repo's root dir:
cd ./.devcontainer

# build and run the environment with docker-compose
docker build -t hexkit .
docker run -it hexkit /bin/bash

```

## License
This repository is free to use and modify according to the [Apache 2.0 License](./LICENSE).

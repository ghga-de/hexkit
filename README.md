[![PyPI version shields.io](https://img.shields.io/pypi/v/hexkit.svg)](https://pypi.python.org/pypi/hexkit/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/hexkit.svg)](https://pypi.python.org/pypi/hexkit/)
![tests](https://github.com/ghga-de/hexkit/actions/workflows/tests.yaml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/ghga-de/hexkit/badge.svg?branch=main)](https://coveralls.io/github/ghga-de/hexkit?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

# hexkit

*A chassis library for building domain-focused, infrastructure-agnostic,
and event-driven microservices in Python*

Read the short summary below, or jump straight to our full
📖 **[User Guide](https://ghga-de.github.io/hexkit/user-guide/)** to learn the abstract
concepts and patterns as well as the concrete infrastructure integrations and testing
tools that hexkit provides for building better microservices.

<!--
NOTE: This README doubles as the landing page of the documentation site built with
great-docs. Therefore, all links must be absolute URLs — repo-relative links like
./examples or ./src would resolve correctly on GitHub but break on the site.
-->

## In a Nutshell

hexkit implements the *Triple Hexagonal Architecture* pattern, an optimization of the
ordinary
[Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/) for
use with microservices: adapters (as per the hexagonal architecture) are divided into
two parts. One part called *translator* is specific to one individual microservice
and its domain-oriented ports. The other part called *provider* is service-agnostic
but specific to one technology of the surrounding infrastructure. Both parts interact
through a high-level interface called *protocol* (not to be confused with Python's
typing.Protocol) which is neither specific to the technology nor to the microservice.

As a chassis lib, hexkit reduces redundancy and boilerplate across microservices by
providing the service-independent building blocks — protocols and providers — ready
to use. The only task that remains for an individual service is to implement
service-specific translators between the service's ports and the general-purpose
protocols (in addition to implementing the domain functionality of the service, of
course). For an in-depth introduction to the pattern, please read the
[Architectural Concepts](https://ghga-de.github.io/hexkit/user-guide/arch_concepts/)
chapter of the User Guide.

The following protocols and providers are currently available:

| Protocol           | Providers                              |
| ------------------ | -------------------------------------- |
| Event Publishing   | Apache Kafka, MongoDB + Kafka (outbox) |
| Event Subscription | Apache Kafka                           |
| Data Access Object | MongoDB                                |
| Object Storage     | S3(-compatible)                        |
| Key-Value Store    | MongoDB, Redis, S3, HashiCorp Vault    |

In-memory implementations and per-backend test utilities are also included to
support testing without real infrastructure.

You are not forced to go all-in on the idea of Triple Hexagonal Architecture.
You can use it just for the technologies where you see benefits and use another
approach for the rest. For example, you could use hexkit for simplifying the exchange
of events between microservices but use a classical web framework such as FastAPI for
designing REST APIs and an ORM like SQLAlchemy for interacting with databases.

## Getting Started

The 📖 [User Guide](https://ghga-de.github.io/hexkit/user-guide/) covers all
protocols, providers, testing utilities, and observability tools with example code,
and the [API Reference](https://ghga-de.github.io/hexkit/reference/) documents all
public classes and functions.

For a complete example service built with hexkit, have a look at the
[stream_calc](https://github.com/ghga-de/hexkit/tree/main/examples/stream_calc)
example application. We also put a lot of effort into making the code
self-documenting: you can find the protocols being defined at
[src/hexkit/protocols](https://github.com/ghga-de/hexkit/tree/main/src/hexkit/protocols)
and the providers being implemented at
[src/hexkit/providers](https://github.com/ghga-de/hexkit/tree/main/src/hexkit/providers).

## Installation

This package is available at PyPI:
<https://pypi.org/project/hexkit>

You can install it from there using:

```sh
pip install hexkit
```

The following extras are available:

- `akafka`: when using the Apache Kafka-based event publishing or subscription
- `mongodb`: when using MongoDB as backend for the DAO protocol or key-value stores
- `s3`: when interacting with S3-compatible object storages or key-value stores
- `redis`: when using Redis-based key-value stores
- `vault`: when using HashiCorp Vault-based key-value stores
- `opentelemetry`: observability instrumentation (also available per backend, e.g.
  `opentelemetry-akafka`, `opentelemetry-mongodb`, `opentelemetry-fastapi`)
- `test-akafka`, `test-mongodb`, `test-s3`, `test-redis`, `test-vault`: testing
  utils for the respective backend (`test` is a union of all of them)
- `all`: a union of all the above

## Contributing

Contributions are welcome! Please see the
[contribution guide](https://github.com/ghga-de/hexkit/blob/main/CONTRIBUTING.md)
for how to set up the development environment and build the documentation locally.

## Credits

The hexkit library is developed and maintained by the developer team of GHGA. We would especially
like to thank its original author, Kersten Breuer, who came up with the idea for the
library and designed and coded its first versions.

## License

This repository is free to use and modify according to the
[Apache 2.0 License](https://github.com/ghga-de/hexkit/blob/main/LICENSE).

<!--
 Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

# Architectural Concepts

This section introduces some key architectural concepts within hexkit.

Hexkit is a library that facilitates the implementation of the Triple Hexagonal Architecture pattern. This pattern is an extension of the traditional Hexagonal Architecture (also known as Ports and Adapters pattern) that addresses code redundancy issues commonly found in microservice environments.

To understand the Triple Hexagonal Architecture, it's important to first understand the traditional Hexagonal Architecture that it builds upon. Additionally, hexkit supports and integrates with other architectural patterns commonly used in modern microservice architectures, including event-driven communication and robust error handling mechanisms.

## Concepts

- [Hexagonal Architecture](hexagonal_arch.md) - the foundational architecture pattern
- [Triple Hexagonal Architecture](triple_hexagonal_arch.md) - hexkit's extension to address microservice code redundancy
- [Event Driven Architecture](event_driven_arch.md) - Communication patterns supported by hexkit for decoupled microservices
- [Dead Letter Queue](dlq.md) - Error handling and reliability patterns integrated with hexkit

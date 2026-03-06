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

# Protocols

Protocols define abstract interfaces (ports) used by applications and adapters.
Providers implement these protocols for specific backends.

## Supported Protocols

### KeyValueStoreProtocol

The [`KeyValueStoreProtocol`](../api/hexkit/protocols/kvstore.html) defines
asynchronous operations for storing and retrieving values by key. It is generic
over the value type `V`, which is determined by the concrete store
implementation. Keys are ordinary `str` values, i.e. normal Unicode strings.

Its core operations are:

- `get(key: str, default: V | None = None) -> V | None`: Retrieve a value by key.
    Returns `default` (or `None`) when the key is not present.
- `set(key: str, value: V) -> None`: Store a value for a key.
- `delete(key: str) -> None`: Remove a key/value pair if present.
- `exists(key: str) -> bool`: Check whether a key exists.

The protocol is implemented by multiple provider families:

- in-memory testing providers: [`InMem*KeyValueStore`](../providers/testing/kvstore.md)
- Redis providers: [`Redis*KeyValueStore`](../providers/redis/kvstore.md)
- MongoDB providers: [`MongoDb*KeyValueStore`](../providers/mongodb/kvstore.md)
- S3 providers: [`S3*KeyValueStore`](../providers/s3/kvstore.md)
- Vault providers: [`Vault*KeyValueStore`](../providers/vault/kvstore.md)

Across these families, the same value-type variants are used:

- `*BytesKeyValueStore`: Stores raw `bytes` values.
- `*StrKeyValueStore`: Stores `str` values.
- `*JsonKeyValueStore`: Stores JSON objects.
- `*DtoKeyValueStore`: Stores Pydantic model instances (DTOs).

This naming convention is backend-agnostic, so switching provider families does
not require changing your value model at the protocol level.

Choose the provider family based on the operational properties you need:
in-memory for tests, Redis for fast access to small hot values, MongoDB for
durable structured values, S3 for large or opaque payloads, and Vault for
secrets or other sensitive values.

Choose the value-type variant based on how much structure and validation you
want: `Bytes` for binary or already-serialized payloads, `Str` for plain text,
`Json` for flexible schema-light objects, and `Dto` for schema-validated typed
models.

### (one section for each protocol)

TBD

## Adding Protocols

TBD

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

# In-Memory KV Store Providers

In-memory providers implement `KeyValueStoreProtocol` for lightweight test setups
without external infrastructure dependencies.

Available variants are `InMemBytesKeyValueStore`, `InMemStrKeyValueStore`,
`InMemJsonKeyValueStore`, and `InMemDtoKeyValueStore`.

API reference: [hexkit.providers.testing](../../api/hexkit/providers/testing.html)

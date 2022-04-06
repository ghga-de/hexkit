<!--
 Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

# Unstructured Notes
Eventually this will be translated into proper documentation.

- Inbound providers should have a `run` method that takes zero arguments
- If an inbound protocol contains coroutines, the responsibility for managing the async 
  event loop lies with the provider. I.e. the provider's `run` method MUST NOT be a
  coroutine.
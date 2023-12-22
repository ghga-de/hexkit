<!--
 Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

# Template File Lists

This directory contains multiple text files that are listing paths to other files
of this repository. The listed files are affected in different ways by template updates
as explained in the following.

## `static_files.txt`
The files listed here are synced with their counterparts in the template. They should
never be modified manually.

## `static_files_ignore.txt`
To opt out of template updates just for individual files declared as static
(e.g. because you would like manually modify them), you may add them to this list.

## `mandatory_files.txt`
The contents of the files listed here are not synced with the template, however, upon
every template update it is checked that the files exist. You should modify them
manually to the needs of your repository.

## `mandatory_files_ignore.txt`
To opt out of existence checks for individual files declared as mandatory, you may add
them to this list.

## `deprecated_files.txt`
Files listed here must not exist in your repository and are automatically deleted upon
a template update.

## `deprecated_files_ignore.txt`
If you would like to keep files declared as deprecated, you may add them to this list.

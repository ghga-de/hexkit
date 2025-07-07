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
"""Utils for defining and applying database migrations"""

import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Optional, Union

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from hexkit.providers.mongodb.provider import document_to_dto, dto_to_document

log = logging.getLogger(__name__)

Document = dict[str, Any]


def validate_doc(doc: Document, *, model: type[BaseModel], id_field: str):
    """Ensure that new content passes model validation.

    Also check that `dto_to_document` results in the same document.

    Raises:
    - `ValidationError`: If the doc cannot be used to populate the model.
    - `RuntimeError`: If the model can be populated, but there's a mismatch.
    """
    # document_to_dto() will modify the doc dict, so don't use the original dict
    doc_copy = {**doc}
    as_model = document_to_dto(doc_copy, id_field=id_field, dto_model=model)
    doc_from_model = dto_to_document(as_model, id_field=id_field)
    if doc != doc_from_model:
        raise RuntimeError(
            f"Doc validation failed for model '{model.__name__}',"
            + f" expected: {str(doc_from_model)}, but got {doc}. Ensure the model"
            + " definition is up-to-date."
        )


class MigrationDefinition:
    """Contains all logic to migrate the database from one version to the next."""

    version: int

    def __init__(
        self,
        *,
        db: AsyncIOMotorDatabase,
        unapplying: bool,
        is_final_migration: bool,
    ):
        """Instantiate the MigrationDefinition.

        Subclass overrides need to call `super().__init__` or include its code.
        """
        if not self.version:
            raise ValueError("Migration version has not been assigned")

        self._db = db
        self._temp_prefix = f"tmp_v{self.version}{'_unapply' if unapplying else ''}"
        self._new_prefix = f"{self._temp_prefix}_new"
        self._old_prefix = f"{self._temp_prefix}_old"
        self._log_blurb = f"for {'downgrade' if unapplying else 'upgrade'} to DB version {self.version}"

        # Used to determine if it should be safe to use model definitions for validation
        self._is_final_migration = is_final_migration

        # Tracks which collections have had indexes copied over from old collections
        self._indexes_copied: set[str] = set()

        # Tracks which collections have been staged
        self._staged_collections: set[str] = set()

    @asynccontextmanager
    async def auto_finalize(
        self, coll_names: Union[str, list[str]], copy_indexes: bool = False
    ):
        """Use within `apply()` or `unapply()` as a context manager to automatically
        stage the temporary migrated collections for the specified collection names and
        then drop the old collections. Set `copy_indexes` to True if the indexes are
        expected to be identical between the old and new collection versions.

        Should be used for most migrations, but complex migrations might need to take
        a more manual approach. For that reason, this context manager is optional.

        If an error occurs during the migration process, staged changes will be unstaged
        and dropped. If a subsequent error occurs during cleanup, it is logged with a
        recommendation to restore the database.
        """
        try:
            # Yield to run the actual migration
            yield
            await self.stage_new_collections(coll_names)

            # Copy indexes if needed
            if copy_indexes:
                await self.auto_copy_indexes(coll_names=coll_names)

            # Drop old collections. Don't do the index copy check unless we perform the
            #  index copying via this method. Otherwise we can't be sure it wasn't
            #  handled some other way
            await self.drop_old_collections(enforce_indexes=copy_indexes)
        except BaseException:
            try:
                # Attempt to revert collections back to original state
                for coll_name in coll_names:
                    # If the collection was staged already, unstage it
                    if coll_name in self._staged_collections:
                        await self.unstage_collection(coll_name)
                    # The only collections to drop are the tmp_vN_new_ collections
                    #  because the tmp_vN_old_ collections were renamed to original name
                    # If the new tmp collection was never created, this has no effect
                    await self._db.drop_collection(self.new_temp_name(coll_name))
            except BaseException as exc_in_cleanup:
                log.critical(
                    "Error occurred while cleaning up migration failure. State cannot"
                    + " be assured to be recoverable. Database restore recommended."
                    + " Exception info: %s",
                    str(exc_in_cleanup),
                )
                raise
            log.critical("Migration failed but cleanup was successful.")
            raise

    @staticmethod
    def _add_prefix(name: str, prefix: str) -> str:
        """Adds a prefix to a string or returns the string unchanged."""
        if name.startswith(prefix):
            return name
        return f"{prefix}_{name}"

    def new_temp_name(self, coll_name: str) -> str:
        """Add `self._new_prefix` to a plain collection name."""
        return self._add_prefix(coll_name, self._new_prefix)

    def old_temp_name(self, coll_name: str) -> str:
        """Add `self._old_prefix` to plain collection name."""
        return self._add_prefix(coll_name, self._old_prefix)

    def get_new_temp_names(self, coll_name: list[str]) -> list[str]:
        """Add `self._new_prefix` to a list of plain collection names."""
        return [self.new_temp_name(name) for name in coll_name]

    def get_old_temp_names(self, coll_name: list[str]) -> list[str]:
        """Add `self._old_prefix` to a list of plain collection names."""
        return [self.old_temp_name(name) for name in coll_name]

    async def migrate_docs_in_collection(  # noqa: PLR0913
        self,
        *,
        coll_name: str,
        change_function: Callable[[Document], Awaitable[Document]],
        validation_model: Optional[type[BaseModel]] = None,
        id_field: str = "",
        force_validate: bool = False,
        batch_size: int = 1000,
    ):
        """Migrate a collection by calling `change_function` on each document within.

        If `validation_model` is supplied, model will be used to cross-check the
        resulting doc data when this is the last migration to be applied/unapplied OR
        `always_validate` is True.

        `batch_size` controls the size of bulk inserts as well as the max number
        of documents retrieved at a time by the cursor.
        """
        if coll_name in self._staged_collections:
            raise RuntimeError("Collections already staged, changes shouldn't be made.")

        old_collection = self._db[coll_name]
        method = change_function

        # Drop the temp collection first to make sure we're starting fresh.
        temp_new_coll_name = self.new_temp_name(coll_name)
        await self._db.drop_collection(temp_new_coll_name)
        temp_new_collection = self._db[temp_new_coll_name]

        # Cursor automatically retrieves batches as needed - we just handle insertion
        inserts = []
        async for doc in old_collection.find(batch_size=batch_size):
            output_doc = await method(doc)

            # do validation against model only if we're on the last migration because
            # the model defined in code is not guaranteed to match until that time
            if validation_model and (self._is_final_migration or force_validate):
                validate_doc(output_doc, model=validation_model, id_field=id_field)

            # Queue the insert and flush if ready
            inserts.append(output_doc)
            if len(inserts) >= batch_size:
                await temp_new_collection.insert_many(inserts)
                inserts.clear()

        # Flush any waiting inserts
        if inserts:
            await temp_new_collection.insert_many(inserts)

        log.debug("Changes applied to collection '%s' %s", coll_name, self._log_blurb)

    async def stage_collection(self, original_coll_name: str):
        """Stage a single collection.

        Do not call until finished with all changes to the collection.
        """
        # Don't do anything if it's already staged
        if original_coll_name in self._staged_collections:
            return

        # Don't do anything if the collection doesn't exist
        if original_coll_name not in await self._db.list_collection_names():
            log.warning(
                "Skipped staging for collection '%s' because it was not found.",
                original_coll_name,
            )
            return

        # Rename the old collection by giving it a prefix
        # e.g. "users" -> "tmp_v7_old_users"
        temp_old_coll_name = self.old_temp_name(original_coll_name)
        old_collection = self._db[original_coll_name]
        await old_collection.rename(temp_old_coll_name, dropTarget=True)

        # Rename the new, temp collection by removing its prefix
        # e.g. "tmp_v7_new_users" -> "users"
        temp_new_coll_name = self.new_temp_name(original_coll_name)
        new_collection = self._db[temp_new_coll_name]
        await new_collection.rename(original_coll_name)

        # Mark this collection as staged
        self._staged_collections.add(original_coll_name)
        log.debug("Staged changes for collection %s", original_coll_name)

    async def unstage_collection(self, original_coll_name: str):
        """Reverse steps from `stage_collection()`"""
        # Don't do anything if the collection doesn't exist
        if original_coll_name not in await self._db.list_collection_names():
            log.warning("Collection '%s' not found, can't unstage.", original_coll_name)
            return

        # Add the prefix back to the new collection
        # e.g. "users" -> "tmp_v7_new_users"
        temp_new_coll_name = self.new_temp_name(original_coll_name)
        new_collection = self._db[original_coll_name]
        await new_collection.rename(temp_new_coll_name, dropTarget=True)

        # Remove the prefix from the old collection
        # e.g. "tmp_v7_old_users" -> "users"
        temp_old_coll_name = self.old_temp_name(original_coll_name)
        old_collection = self._db[temp_old_coll_name]
        await old_collection.rename(original_coll_name)

        # Remove this collection from the "staged" tracking set
        self._staged_collections.remove(original_coll_name)
        log.debug("Unstaged changes for collection %s", original_coll_name)

    async def stage_new_collections(self, original_coll_names: Union[str, list[str]]):
        """Rename old collections to temporarily move them aside without dropping them,
        then remove the temporary prefix from the migrated collections.

        Do not call until finished making changes to (migrating) the collections.
        """
        if isinstance(original_coll_names, str):
            original_coll_names = [original_coll_names]
        for coll_name in original_coll_names:
            await self.stage_collection(coll_name)
        log.info("Temp collections staged %s", self._log_blurb)

    async def copy_indexes(self, *, source_coll_name: str, dest_coll_name: str):
        """Copy the indexes from `source_coll_name` to `dest_coll_name`.

        This function can be used for manual index copying when `auto_copy_indexes`
        doesn't suffice. Normally, prefer to use `auto_copy_indexes`.
        """
        source_collection = self._db[source_coll_name]
        index_info = await source_collection.index_information()
        count = len(index_info)

        if count:
            dest_collection = self._db[dest_coll_name]

            # Copy indexes along with any options (e.g. 'unique', 'background', etc.)
            for name, index in index_info.items():
                key = index.pop("key")
                index.pop("v", "")
                index.pop("ns", "")
                await dest_collection.create_index(keys=key, name=name, **index)

            log.debug(
                "Copied %i indexes from %s to %s",
                count,
                source_coll_name,
                dest_coll_name,
            )
        else:
            log.debug(
                "No indexes available to copy from %s to %s",
                source_coll_name,
                dest_coll_name,
            )

    async def auto_copy_indexes(self, *, coll_names: Union[str, list[str]]):
        """Copy the indexes from old collections to new, and remember that the indexes
        have been copied for these collections.
        """
        if isinstance(coll_names, str):
            coll_names = [coll_names]
        for coll_name in coll_names:
            # If the collection is already staged, use appropriate names
            if coll_name in self._staged_collections:
                old_coll_name = self.old_temp_name(coll_name)
                new_coll_name = coll_name
            else:
                old_coll_name = coll_name
                new_coll_name = self.new_temp_name(coll_name)

            await self.copy_indexes(
                source_coll_name=old_coll_name, dest_coll_name=new_coll_name
            )
            self._indexes_copied.add(coll_name)

    async def drop_old_collections(self, *, enforce_indexes: bool):
        """Drop the old, pre-migration version of all staged collections.

        Args
        - `enforce_indexes`: Raise an error if indexes haven't been copied over to the
            replacement collections. This is not always useful, since the collections
            might undergo changes that make old indexes obsolete. This should be set to
            True for migrations that don't involve changes to the collections' indexes.
        """
        if enforce_indexes and not self._indexes_copied:
            raise RuntimeError("Indexes have not been applied to staged collections")

        for coll_to_drop in list(self._staged_collections):
            old_temp_name = self.old_temp_name(coll_to_drop)
            collection = self._db[old_temp_name]
            await collection.drop()
            log.debug(
                "Dropped old collection for '%s' ('%s')", coll_to_drop, old_temp_name
            )
            self._staged_collections.remove(coll_to_drop)

    @abstractmethod
    async def apply(self):
        """Make the changes required to move the DB version to `self.version`."""
        ...

    async def unapply(self):
        """Placeholder for a method to reverse the migration changes.

        To implement, additionally subclass Reversible:

        ```
        class MyMigration(MigrationDefinition, Reversible):
            ...
        ```
        """
        raise NotImplementedError()


class Reversible(ABC):
    """Mixin class to mark a migration class as reversible."""

    @abstractmethod
    async def unapply(self):
        """Reverse changes made by `apply()`."""
        ...

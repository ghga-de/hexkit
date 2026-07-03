#!/usr/bin/env python3

# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Quarto pre-render hook: fix great-docs "view source" URLs for the src/ layout.

great-docs builds the reference "Source" links from griffe's package-relative
path (e.g. ``hexkit/providers/.../file.py``), which omits the ``src/`` prefix
that the file actually has in the repository. The resulting GitHub blob URLs
therefore 404. This hook rewrites

    /blob/<ref>/hexkit/...   ->   /blob/<ref>/src/hexkit/...

in the generated ``reference/*.qmd`` files, after great-docs has written them and
before Quarto renders them to HTML. It is idempotent (a URL that already contains
``src/hexkit/`` is left untouched).

Registered via ``pre_render:`` in ``great-docs.yml``. Quarto runs it with the
build project directory as CWD and also exposes ``QUARTO_PROJECT_DIR``.

TODO: Remove this hook once great-docs ships a release > 0.14.1 that includes the
upstream fix (posit-dev/great-docs PR #245, merged 2026-06-30), which computes the
source path relative to the repo root and preserves the ``src/`` prefix natively.
As of 0.14.1 (released 2026-06-25) the fix is merged but unreleased.

NOTE: ``branch: main`` is pinned in great-docs.yml, so refs never contain a
slash; the ``[^/]+`` ref matcher below is sufficient. Revisit if the source
branch ever becomes a slashed ref (e.g. ``release/1.2``).
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

PACKAGE = "hexkit"
SRC_PREFIX = "src"


def _project_dir() -> Path:
    env = os.environ.get("QUARTO_PROJECT_DIR")
    if env:
        return Path(env)
    # Script is copied to <project>/scripts/, so the project dir is its grandparent.
    return Path(__file__).resolve().parent.parent


def main() -> int:
    reference_dir = _project_dir() / "reference"
    if not reference_dir.is_dir():
        print(f"[fix_source_link_paths] no reference/ dir at {reference_dir}; skipping")
        return 0

    pattern = re.compile(rf"(/blob/[^/]+/){re.escape(PACKAGE)}/")
    replacement = rf"\g<1>{SRC_PREFIX}/{PACKAGE}/"

    changed = 0
    for qmd in sorted(reference_dir.glob("*.qmd")):
        text = qmd.read_text(encoding="utf-8")
        new_text = pattern.sub(replacement, text)
        if new_text != text:
            qmd.write_text(new_text, encoding="utf-8")
            changed += 1

    print(
        f"[fix_source_link_paths] reinserted src/ prefix in {changed} reference file(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

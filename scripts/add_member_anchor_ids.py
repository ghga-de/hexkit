#!/usr/bin/env python3
"""Quarto pre-render hook: add the anchors that great-docs' cross-links point to.

great-docs' automatic API cross-linker (interlinks and inline-code autolinks in
user-guide pages) builds links from the ``objects.json`` inventory, whose URIs
carry fully qualified fragments such as

    reference/protocols.dao.Dao.html#hexkit.protocols.dao.Dao.insert

The generated reference pages, however, only carry Pandoc's auto-generated
heading ids (``id="insert"``), so those links land on the correct page but do
not scroll to the member. This hook closes the gap from the target side: for
every inventory entry it injects an invisible Pandoc span
(``[]{#<qualified name>}``) into the corresponding generated
``reference/*.qmd`` file, right before the member's heading (or after the
front matter for page-level objects). The existing ids, summary tables, and
TOC are left untouched.

It also fixes a sibling mismatch within the reference pages themselves: their
member summary tables link by literal names (``#BucketNotFoundError``,
``#__init__``) while Pandoc's auto ids are lowercased with leading punctuation
stripped (``#bucketnotfounderror``, ``#init``). A literal-name anchor is added
wherever the two differ.

Registered via ``pre_render:`` in ``great-docs.yml``. Quarto runs it with the
build project directory as CWD and also exposes ``QUARTO_PROJECT_DIR``. It is
idempotent (an anchor that is already present is not added again).

TODO: Remove this hook once great-docs resolves the fragment mismatch upstream
(unfixed as of 0.14.1; no issue filed yet when this hook was written).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

PACKAGE = "hexkit"

# Member headings look like:  ### [insert()]{.doc-object-name ...}
# The name may carry Pandoc escapes (e.g. ``\__init__``) and a ``()`` suffix.
HEADING_RE = re.compile(r"^#{2,4} \[(?P<name>.+?)\]\{\.doc-object-name")


def _project_dir() -> Path:
    env = os.environ.get("QUARTO_PROJECT_DIR")
    if env:
        return Path(env)
    # Script is copied to <project>/scripts/, so the project dir is its grandparent.
    return Path(__file__).resolve().parent.parent


def _clean_heading_name(raw: str) -> str:
    """Normalize a heading name: drop Pandoc escapes and a trailing ``()``."""
    return raw.replace("\\", "").removesuffix("()")


def _insert_anchor(lines: list[str], index: int, fragment: str) -> None:
    """Insert an anchor span as its own paragraph before ``lines[index]``."""
    lines[index:index] = [f"[]{{#{fragment}}}", ""]


def _literal_name_insertions(
    heading_lines: dict[str, int], text: str
) -> list[tuple[int, str]]:
    """Anchors for the pages' own member summary tables.

    Those tables link by literal (case-preserved) names, e.g.
    ``#BucketNotFoundError`` or ``#__init__``, while Pandoc's auto-generated
    heading ids are lowercased with leading punctuation stripped. Return an
    anchor insertion for every member where the two differ.
    """
    return [
        (lineno, member)
        for member, lineno in heading_lines.items()
        if (member.lower() != member or member.startswith("_"))
        and f"[]{{#{member}}}" not in text
    ]


def _add_anchors_to_page(qmd: Path, fragments: list[str]) -> tuple[int, list[str]]:
    """Inject anchors for the given fragments; returns (added, unplaced)."""
    text = qmd.read_text(encoding="utf-8")
    lines = text.split("\n")
    page_object = f"{PACKAGE}.{qmd.stem}"

    # Map member name -> line number of its heading (first occurrence wins).
    heading_lines: dict[str, int] = {}
    for lineno, line in enumerate(lines):
        match = HEADING_RE.match(line)
        if match:
            heading_lines.setdefault(_clean_heading_name(match["name"]), lineno)

    insertions: list[tuple[int, str]] = []
    unplaced: list[str] = []

    for fragment in fragments:
        if f"[]{{#{fragment}}}" in text:
            continue  # already present (idempotency)
        if fragment == page_object:
            # Page-level object: anchor right after the closing front-matter fence.
            end = lines.index("---", 1) if lines and lines[0] == "---" else 0
            insertions.append((end + 1, fragment))
        else:
            member = fragment.rsplit(".", 1)[-1]
            if member in heading_lines:
                insertions.append((heading_lines[member], fragment))
            else:
                unplaced.append(fragment)

    insertions.extend(_literal_name_insertions(heading_lines, text))

    # Insert bottom-up so earlier line numbers stay valid.
    for lineno, anchor in sorted(insertions, key=lambda pair: pair[0], reverse=True):
        _insert_anchor(lines, lineno, anchor)

    if insertions:
        qmd.write_text("\n".join(lines), encoding="utf-8")
    return len(insertions), unplaced


def main() -> int:
    project_dir = _project_dir()
    inventory_path = project_dir / "objects.json"
    reference_dir = project_dir / "reference"
    if not inventory_path.is_file() or not reference_dir.is_dir():
        print("[add_member_anchor_ids] no objects.json or reference/ dir; skipping")
        return 0

    items = json.loads(inventory_path.read_text(encoding="utf-8")).get("items", [])

    # Group the promised fragments by target reference page.
    fragments_by_page: dict[str, list[str]] = {}
    for item in items:
        uri = item.get("uri", "")
        page, _, fragment = uri.partition("#")
        if not fragment or not page.startswith("reference/"):
            continue
        page_stem = page.removeprefix("reference/").removesuffix(".html")
        page_fragments = fragments_by_page.setdefault(page_stem, [])
        if fragment not in page_fragments:
            page_fragments.append(fragment)

    total_added = 0
    all_unplaced: list[str] = []
    missing_pages: list[str] = []
    for page_stem, fragments in sorted(fragments_by_page.items()):
        qmd = reference_dir / f"{page_stem}.qmd"
        if not qmd.is_file():
            missing_pages.append(page_stem)
            continue
        added, unplaced = _add_anchors_to_page(qmd, fragments)
        total_added += added
        all_unplaced.extend(unplaced)

    print(f"[add_member_anchor_ids] added {total_added} qualified anchor(s)")
    if all_unplaced:
        print(
            f"[add_member_anchor_ids] warning: no heading found for "
            f"{len(all_unplaced)} fragment(s): {', '.join(sorted(all_unplaced)[:5])}"
            + (" ..." if len(all_unplaced) > 5 else "")
        )
    if missing_pages:
        print(
            f"[add_member_anchor_ids] warning: {len(missing_pages)} inventory "
            f"page(s) without a qmd file: {', '.join(sorted(missing_pages)[:5])}"
            + (" ..." if len(missing_pages) > 5 else "")
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

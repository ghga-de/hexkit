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

"""Generates the redirect site that replaces the retired hexkit docs site.

hexkit has moved into the GHGA monorepo (https://github.com/ghga-de/ghga, under
libs/hexkit), which now publishes the same documentation site to
https://ghga-de.github.io/ghga/hexkit/. This repository is being archived, but its
GitHub Pages deployment must keep resolving: the old URL is baked into the immutable
PyPI metadata of hexkit 9.0.0 and 9.0.1.

Archiving disables Actions while leaving Pages serving whatever was deployed last, so
the final deploy from this repository has to be a redirect site, and it can never be
redone. That is also why the path list is a committed file (`pages_paths.txt`, taken
from the live sitemap of the old site) rather than something derived by rebuilding the
docs: a committed list cannot break, whereas a build depends on great-docs and the dev
lock still resolving on a runner years from now.

The new site contains all of the old paths 1:1, so the mapping is a pure prefix
rewrite: https://ghga-de.github.io/hexkit/<path>
      -> https://ghga-de.github.io/ghga/hexkit/<path>

Each generated stub carries all of:
  - a canonical link, transferring the page to its new URL for search engines,
  - a script doing location.replace(target + location.search + location.hash), which
    preserves the API reference anchors (#hexkit.protocols.dao.Dao.insert) that a bare
    meta refresh would drop,
  - a meta refresh as the no-JS fallback,
  - a visible link, for a reader who sees the page for a moment.

In addition, a 404.html catch-all rewrites any unmatched path (assets, page names from
older versions of the site, hand-typed URLs). It complements the stubs rather than
replacing them: it is served with HTTP 404, whereas the stubs return 200.

Usage: ./scripts/generate_redirect_site.py [output_dir]
"""

import json
import sys
from html import escape
from pathlib import Path

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()

# The paths of the old site, one per line, relative to the old site root.
PATHS_FILE_PATH = Path(__file__).parent / "pages_paths.txt"

DEFAULT_OUTPUT_DIR = REPO_ROOT_DIR / "_redirect_site"

# Base path under which the old site is served (this repository's Pages base path),
# without a trailing slash.
OLD_BASE_PATH = "/hexkit"

# Root of the site that took over, with trailing slash.
NEW_BASE_URL = "https://ghga-de.github.io/ghga/hexkit/"

# Number of URLs the live sitemap of the old site listed, used as a sanity check.
EXPECTED_NUM_PAGES = 148

STUB_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved – hexkit documentation</title>
<link rel="canonical" href="{target_attr}">
<script>
  // Redirect in JS first so that the query string and, above all, the fragment
  // survive: the API reference anchors are exactly what people paste into issues.
  location.replace({target_js} + location.search + location.hash);
</script>
<meta http-equiv="refresh" content="0; url={target_attr}">
</head>
<body>
<h1>This page has moved</h1>
<p>
  The hexkit documentation now lives in the GHGA monorepo. This page is at
  <a href="{target_attr}">{target_text}</a>.
</p>
</body>
</html>
"""

NOT_FOUND_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved – hexkit documentation</title>
<script>
  // GitHub Pages serves this page for every path the redirect stubs do not cover.
  // Map it to the same path under the new site, keeping query string and fragment.
  var base = {old_base_js};
  var path = location.pathname;
  var rest =
    path === base ? ""
    : path.indexOf(base + "/") === 0 ? path.slice(base.length + 1)
    : path.replace(/^\\//, "");
  location.replace({new_base_js} + rest + location.search + location.hash);
</script>
<meta http-equiv="refresh" content="0; url={new_base_attr}">
</head>
<body>
<h1>This page has moved</h1>
<p>
  The hexkit documentation now lives in the GHGA monorepo, at
  <a href="{new_base_attr}">{new_base_text}</a>.
</p>
</body>
</html>
"""


def read_paths() -> list[str]:
    """Reads the committed page paths, adding the site root itself."""
    lines = PATHS_FILE_PATH.read_text(encoding="utf8").splitlines()
    # An empty line denotes the site root; it is added unconditionally below, since
    # the sitemap lists it but a plain text file cannot hold it unambiguously.
    paths = [""] + [line.strip() for line in lines if line.strip()]
    if len(set(paths)) != len(paths):
        raise ValueError(f"Duplicate paths in {PATHS_FILE_PATH}")
    return paths


def output_path_for(path: str) -> str:
    """Translates a site path into the file to be written for it.

    Directory-style paths (including the root) become an index.html inside them.
    """
    return f"{path}index.html" if path.endswith("/") or not path else path


def write_stub(output_dir: Path, path: str) -> None:
    """Writes the redirect stub for a single page path."""
    target = NEW_BASE_URL + path
    stub_path = output_dir / output_path_for(path)
    stub_path.parent.mkdir(parents=True, exist_ok=True)
    stub_path.write_text(
        STUB_TEMPLATE.format(
            target_attr=escape(target, quote=True),
            target_js=json.dumps(target),
            target_text=escape(target),
        ),
        encoding="utf8",
    )


def write_not_found(output_dir: Path) -> None:
    """Writes the catch-all page for paths that have no stub."""
    (output_dir / "404.html").write_text(
        NOT_FOUND_TEMPLATE.format(
            old_base_js=json.dumps(OLD_BASE_PATH),
            new_base_js=json.dumps(NEW_BASE_URL),
            new_base_attr=escape(NEW_BASE_URL, quote=True),
            new_base_text=escape(NEW_BASE_URL),
        ),
        encoding="utf8",
    )


def generate(output_dir: Path) -> int:
    """Generates the complete redirect site and returns the number of stubs."""
    paths = read_paths()
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in paths:
        write_stub(output_dir, path)
    write_not_found(output_dir)
    # GitHub Pages runs Jekyll on the uploaded tree by default; nothing here needs it.
    (output_dir / ".nojekyll").write_text("", encoding="utf8")
    return len(paths)


def main() -> None:
    """Generates the redirect site into the directory given on the command line."""
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    num_stubs = generate(output_dir)
    print(f"Wrote {num_stubs} redirect stubs plus 404.html to {output_dir}")
    if num_stubs != EXPECTED_NUM_PAGES:
        sys.exit(
            f"Error: expected {EXPECTED_NUM_PAGES} pages, but got {num_stubs}."
            f" Check {PATHS_FILE_PATH}."
        )


if __name__ == "__main__":
    main()

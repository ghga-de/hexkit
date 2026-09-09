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
PyPI metadata of hexkit 9.0.0 and 9.0.1, both in `Documentation =` and in the deep
links of the README rendered on those PyPI pages.

Archiving disables Actions while leaving Pages serving whatever was deployed last, so
the final deploy from this repository has to be a redirect site, and it can never be
redone. That is why this generator depends on nothing but the standard library: it
must keep working without great-docs, Quarto, or a dependency lock that still resolves.

The site consists of just two pages:

  - index.html, for the site root, which is the URL in the PyPI metadata. It carries a
    canonical link, transferring the root to its new address for search engines.
  - 404.html, which GitHub Pages serves for every other path. It rewrites the path it
    was asked for onto the new site, so deep links keep their destination.

Both redirect via location.replace(target + location.search + location.hash) rather
than by meta refresh alone, because that preserves the fragment: the API reference
anchors (#hexkit.protocols.dao.Dao.insert) are exactly the links people paste into
issues, and a bare meta refresh drops them. The meta refresh remains as the no-JS
fallback, and a visible link tells a reader who sees the page for a moment where it
went.

Note that 404.html is served with HTTP 404 rather than 200. Per-page stubs returning
200, with a canonical link each, were considered and deliberately dropped: they would
have frozen a snapshot of today's page names into a repository that can never be
changed again. Rewriting the path instead keeps the information in the URL and leaves
it to the new site — which stays maintainable — to decide what to do with a page that
has since been renamed or removed.
"""

import json
import sys
from html import escape
from pathlib import Path

REPO_ROOT_DIR = Path(__file__).parent.parent.resolve()

DEFAULT_OUTPUT_DIR = REPO_ROOT_DIR / "_redirect_site"

# Base path under which the old site is served (this repository's Pages base path),
# without a trailing slash.
OLD_BASE_PATH = "/hexkit"

# Root of the site that took over, with trailing slash.
NEW_BASE_URL = "https://ghga-de.github.io/ghga/hexkit/"

INDEX_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved – hexkit documentation</title>
<link rel="canonical" href="{new_base_attr}">
<script>
  // Redirect in JS first so that the query string and fragment survive.
  location.replace({new_base_js} + location.search + location.hash);
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

NOT_FOUND_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Moved – hexkit documentation</title>
<script>
  // GitHub Pages serves this page for every path other than the site root. Map it to
  // the same path under the new site, keeping the query string and the fragment (the
  // API reference anchors that people paste into issues).
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
  The hexkit documentation now lives in the GHGA monorepo. If you are not redirected,
  find this page under <a href="{new_base_attr}">{new_base_text}</a>.
</p>
</body>
</html>
"""


def generate(output_dir: Path) -> None:
    """Generates the complete redirect site."""
    output_dir.mkdir(parents=True, exist_ok=True)
    substitutions = {
        "old_base_js": json.dumps(OLD_BASE_PATH),
        "new_base_js": json.dumps(NEW_BASE_URL),
        "new_base_attr": escape(NEW_BASE_URL, quote=True),
        "new_base_text": escape(NEW_BASE_URL),
    }
    (output_dir / "index.html").write_text(
        INDEX_TEMPLATE.format(**substitutions), encoding="utf8"
    )
    (output_dir / "404.html").write_text(
        NOT_FOUND_TEMPLATE.format(**substitutions), encoding="utf8"
    )
    # GitHub Pages runs Jekyll on the uploaded tree by default; nothing here needs it.
    (output_dir / ".nojekyll").write_text("", encoding="utf8")


def main() -> None:
    """Generates the redirect site into the directory given on the command line."""
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    generate(output_dir)
    print(f"Wrote index.html and 404.html to {output_dir}")


if __name__ == "__main__":
    main()

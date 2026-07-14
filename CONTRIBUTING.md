# Contributing to hexkit

Thank you for your interest in contributing to hexkit!

Although hexkit is designed as a general-purpose library, it currently contains only
a limited collection of protocol-provider pairs that are of immediate interest to the
authors. We would like to add support for more protocols and technologies over time.

Feel free to develop hexagonal components yourself, whether or not you use the base
classes of hexkit. Not every protocol or provider has to be general-purpose, however,
if they are, please consider contributing them to hexkit.

## Development Environment

For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of VS Code.

To use it, you need Docker and VS Code with the "Remote - Containers" extension
(`ms-vscode-remote.remote-containers`) installed.
Then, you just have to open this repo in VS Code and run the command
`Remote-Containers: Reopen in Container` from the VS Code "Command Palette".

This will give you a full-fledged, pre-configured development environment including:

- infrastructural dependencies of the service (databases, etc.)
- all relevant VS Code extensions pre-installed
- pre-configured linting and auto-formatting
- a pre-configured debugger
- automatic license-header insertion

Additionally, the following convenience command is available inside the devcontainer
(type it in the integrated terminal of VS Code):

- `dev_install` - install the lib with all development dependencies and pre-commit hooks
(please run that if you are starting the devcontainer for the first time
or if added any python dependencies to the [`./pyproject.toml`](./pyproject.toml))

If you prefer not to use VS Code, you could get a similar setup (without the editor
specific features) by running the following commands:

``` bash
# Execute in the repo's root dir:
cd ./.devcontainer

# build and run the environment with docker-compose
docker build -t hexkit .
docker run -it hexkit /bin/bash

```

## Documentation

The narrative docs live under [`./user_guide`](./user_guide) and the site is
configured by [`./great-docs.yml`](./great-docs.yml); it is built with
[Great Docs](https://posit-dev.github.io/great-docs/) (which uses Quarto, already
installed in the devcontainer). Build the site, then serve it locally:

``` bash
# Build the site into great-docs/_site/
great-docs build

# Serve the built site at http://localhost:3000
great-docs preview
```

`preview` only serves the existing build (it builds first only when no build exists
yet) and does not watch for changes. Because `great-docs build` clears and
regenerates the build directory, don't rebuild while a preview is running — instead,
after editing a page or `great-docs.yml`, stop the preview (`Ctrl+C`), re-run
`great-docs build`, and start `great-docs preview` again to view your changes.

> **Note:** great-docs' `build --watch` doesn't track edits to your
> `user_guide/` source (it only watches the generated build directory), so
> rebuild manually as above. A full rebuild takes a while, so don't expect
> your changes to show up immediately.

Note that the repository's `README.md` doubles as the landing page of the
documentation site, so all links in it must be absolute URLs (repo-relative links
would break on the site).

## License

By contributing, you agree that your contributions will be licensed under the
[Apache 2.0 License](./LICENSE).

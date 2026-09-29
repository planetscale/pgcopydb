# Contributing to pgcopydb

PlanetScale maintains this fork directly and does not merge changes back to the
upstream project. Send everything here, not to `dimitri/pgcopydb`. See
[About this fork](README.md#about-this-fork).

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/planetscale/pgcopydb/issues) with
  suggestions for improvements, potential bugs, etc.
* Fork this repository and submit a pull request

## Building from source code

Follow [Build from sources](docs/install.md#build-from-sources) in the
documentation.

## Following our coding conventions

CI pipeline will automatically reject any PRs which do not follow our coding
conventions. The easiest way to ensure your PR adheres to those conventions is
to use the
[citus_indent](https://github.com/citusdata/tools/tree/develop/uncrustify) tool.
This tool uses `uncrustify` under the hood.

```bash
# On debian, run the following to install uncrustify.
# On other distros, use your package manager for a similar command.
sudo apt-get install uncrustify

# Install citus_indent
git clone https://github.com/citusdata/tools.git
cd tools
make uncrustify/.install
```

Once you've done that, you can run the `make indent` command from the top
directory to recursively check and correct the style of any source files in the
current directory. Under the hood, `make indent` will run `citus_indent` for
you.

You can also run the following in the directory of this repository to
automatically format all the files that you have changed before committing:

```bash
cat > .git/hooks/pre-commit << __EOF__
#!/bin/bash
citus_indent --check --diff || { citus_indent --diff; exit 1; }
__EOF__
chmod +x .git/hooks/pre-commit
```

## Running tests

pgcopydb uses docker containers to create test environments. Each test lives
under a separate directory under `tests/` directory at the top directory of the
project.

You can run the following commands to run our tests:

```bash
# To run all the tests
make tests

# To run a single test, e.g. pagila
make tests/pagila
```

## Documentation

User-facing documentation lives in [docs/](docs/) as Markdown, which GitHub
renders directly. When adding a new feature, function, or setting, you are
expected to add relevant documentation change in your pull request.

If you changed the help output of a pgcopydb command, run:

```bash
make update-docs
```

Each documentation page embeds command help between markers:

```
<!-- BEGIN HELP: pgcopydb clone -->
...
<!-- END HELP -->
```

`make update-docs` builds the binary and rewrites the block between each pair of
markers with the current help output. CI fails when a page is out of date, so
run it and commit the result. Do not edit the text inside those markers by hand.

Markdown is linted in CI, and the same check runs locally in a container:

```bash
make lint-docs     # markdownlint, configured by .markdownlint-cli2.yaml
```

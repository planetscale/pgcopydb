# pgcopydb

pgcopydb automates the `pg_dump | pg_restore` pipeline between two running PostgreSQL servers. It copies databases in parallel without intermediate files, builds indexes concurrently, and supports online migration via Change Data Capture (CDC) with logical decoding.

Key capabilities:
- Parallel table COPY with concurrent index builds
- Logical decoding with wal2json for CDC
- Online migration via `pgcopydb clone --follow`
- Schema/table filtering and partitioned table splitting

## Development Environment

**Container-only builds are required.** Do not run `make` or `make bin` on the host — use containers to avoid dependency conflicts.

```bash
PGVERSION=16 make build          # Build in container (Docker)
DOCKER=podman PGVERSION=16 make build  # If using Podman instead of Docker
```

### Build dependencies (installed inside the container)

- `postgresql-server-dev-XX`, `libpq-dev`
- `libgc-dev` (Boehm GC), `libncurses-dev`, `libedit-dev`
- `libssl-dev`, `libkrb5-dev`, `libxml2-dev`, `libxslt1-dev`
- `zlib1g-dev`, `liblz4-dev`, `libzstd-dev`

## Testing

Always run tests before committing. Tests use Docker Compose to spin up PostgreSQL source/target instances.

```bash
PGVERSION=16 make tests          # Run all 17 test suites
PGVERSION=16 make tests/pagila   # Run a specific suite
PGVERSION=16 make tests/unit     # Unit tests
```

Test suites by category:

| Category | Suites |
|----------|--------|
| Core | `pagila`, `pagila-multi-steps`, `pagila-standby`, `unit` |
| Features | `blobs`, `extensions`, `filtering`, `timescaledb` |
| CDC | `cdc-wal2json`, `cdc-test-decoding`, `follow-wal2json`, `follow-9.6`, `follow-data-only` |
| Edge cases | `cdc-endpos-between-transaction`, `endpos-in-multi-wal-txn`, `cdc-low-level` |

## Code Style

pgcopydb uses `citus_indent` (a wrapper around uncrustify). Run formatting before committing:

```bash
make indent
```

Standards: ISO C99 with GNU extensions, 4-space indentation.

CI enforces style — PRs with formatting issues will fail the style check.

## Documentation

Documentation lives in `docs/` and is built with Sphinx (reStructuredText).

**Never manually edit files in `docs/include/*.rst`.** These are auto-generated from CLI help text and manual edits will be overwritten.

When adding or modifying CLI commands:

1. Update help text in the C source files
2. Build in container: `PGVERSION=16 make build`
3. Run `make update-docs` to regenerate `docs/include/*.rst`
4. Commit both the code changes and the generated docs

If `make update-docs` fails locally, use the container binary:

```bash
cat > /tmp/pgcopydb-wrapper.sh << 'EOF'
#!/bin/bash
podman run --rm localhost/pgcopydb:latest pgcopydb "$@"
EOF
chmod +x /tmp/pgcopydb-wrapper.sh
PGCOPYDB=/tmp/pgcopydb-wrapper.sh bash ./docs/update-help-messages.sh
```

## Source Code Map

### Entry points
- `src/bin/pgcopydb/main.c` — application entry point
- `src/bin/pgcopydb/cli_root.c` — root command dispatcher

### Core copy logic
- `copydb.c` — main copy orchestration
- `copydb_schema.c` — schema handling
- `catalog.c` — database catalog queries
- `dump_restore.c` — pg_dump/pg_restore wrapper

### CDC / logical decoding
- `ld_stream.c` — logical decoding streaming
- `ld_transform.c` — WAL JSON to SQL transformation
- `ld_apply.c` — apply changes to target
- `ld_wal2json.c` — wal2json plugin support
- `follow.c` — continuous follow mode orchestration

### Features
- `indexes.c` — concurrent index building
- `blobs.c` — large object handling
- `extensions.c` — extension support
- `filtering.c` — schema/table filtering
- `compare.c` — source/target comparison

### CLI subcommands
- `cli_clone_follow.c` — online migration (`clone --follow`)
- `cli_copy.c` — copy operations
- `cli_stream.c` — streaming/CDC commands
- `cli_compare.c` — comparison commands
- `cli_common.c` — shared CLI options and flags

### Utilities
- `file_utils.c` — file operations
- `parsing_utils.c` — SQL/URI parsing
- `string_utils.c` — string manipulation
- `lock_utils.c` — locking primitives
- `pgcmd.c` — PostgreSQL command execution
- `pgsql_timeline.c` — timeline management

### Vendored libraries (`src/bin/lib/`)
- `sqlite/` — embedded SQLite for state tracking
- `log/` — logging framework
- `parson/` — JSON parsing
- `subcommands.c` — CLI argument parsing
- `uthash/` — hash tables (header-only)
- `pg/` — PostgreSQL utility functions (snprintf, dumputils)

All source paths above are relative to `src/bin/pgcopydb/` unless otherwise noted.

## Architecture

- **Concurrency**: fork-based parallelism — parallel COPY workers, concurrent index builds, separate processes for streaming/transform/apply
- **Memory**: Boehm-Demers-Weiser garbage collector (`libgc`) for automatic memory management
- **Database access**: libpq with separate connections for catalog queries, COPY, index builds, and logical decoding
- **State tracking**: embedded SQLite database stores copy progress, index metadata, CDC state (LSNs, transactions), and filtering rules
- **Security**: parameterized queries throughout, no intermediate file storage of credentials, PIE and stack protection enabled

## Git Workflow

- Keep commit messages short and focused on the "what" and "why"
- Never force push (`git push -f` / `git push --force`)
- Always run `make tests` before committing
- Do not `git add` without explicit confirmation

## Pull Requests

When creating PRs, always target the correct repository explicitly:

```bash
# Correct — explicitly targets planetscale/pgcopydb
gh pr create --repo planetscale/pgcopydb --base main --head branch-name

# Wrong — defaults may target upstream dimitri/pgcopydb
gh pr create --base main --head branch-name
```

Never create PRs against the upstream `dimitri/pgcopydb` repository without explicit instruction.

## CI/CD

**Test pipeline** (`.github/workflows/run-tests.yml`):
- Triggered on pushes, PRs, and manual dispatch
- Matrix: PostgreSQL 16 across all test suites
- Includes style checking and documentation build verification
- 5-minute timeout per test

**Docker publish** (`.github/workflows/docker-publish.yml`):
- Multi-arch: `linux/amd64`, `linux/arm64`
- Registry: `ghcr.io`
- Tags: `:latest` on main, `:vX.Y.Z` on version tags

## Boundaries

**Always:**
- Build inside containers
- Run tests before committing
- Run `make update-docs` when changing CLI commands
- Use `--repo planetscale/pgcopydb` when creating PRs

**Never:**
- Build on the host system
- Manually edit `docs/include/*.rst`
- Force push to any branch
- Create PRs against upstream `dimitri/pgcopydb`

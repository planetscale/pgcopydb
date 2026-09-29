# pgcopydb documentation

pgcopydb is an Open Source Software project. PlanetScale launched this fork in
2026 and maintains it directly. Development happens in public at
[github.com/planetscale/pgcopydb](https://github.com/planetscale/pgcopydb),
where everyone is welcome to open issues and pull requests.

pgcopydb was created by [Dimitri Fontaine](https://github.com/dimitri), whose
design carries this project. PlanetScale no longer targets full parity with the
upstream project, and does not merge this work back to it. Direction comes from
what PlanetScale customers need when they migrate production databases.

## Contents

**Getting started**

- [Introduction](intro.md)
- [Tutorial](tutorial.md)
- [Installing pgcopydb](install.md)

**Design considerations**

- [Features](features.md)
- [Concurrency](concurrency.md)
- [Resuming operations](resume.md)
- [Options for large migrations](operations.md)

**Reference manual**

- [pgcopydb](ref/pgcopydb.md)
- [pgcopydb clone](ref/pgcopydb_clone.md)
- [pgcopydb follow](ref/pgcopydb_follow.md)
- [pgcopydb snapshot](ref/pgcopydb_snapshot.md)
- [pgcopydb compare](ref/pgcopydb_compare.md)
- [pgcopydb copy](ref/pgcopydb_copy.md)
- [pgcopydb dump](ref/pgcopydb_dump.md)
- [pgcopydb restore](ref/pgcopydb_restore.md)
- [pgcopydb list](ref/pgcopydb_list.md)
- [pgcopydb stream](ref/pgcopydb_stream.md)
- [pgcopydb configuration](ref/pgcopydb_config.md)

## How to copy a Postgres database

pgcopydb is a tool that automates copying a PostgreSQL database to another
server. Main use case for pgcopydb is migration to a new Postgres system, either
for new hardware, new architecture, or new Postgres major version.

The idea would be to run `pg_dump -jN | pg_restore -jN` between two running
Postgres servers. To make a copy of a database to another server as quickly as
possible, one would like to use the parallel options of `pg_dump` and still be
able to stream the data to as many `pg_restore` jobs. Unfortunately, this
approach cannot be implemented by using `pg_dump` and `pg_restore` directly, see
[Bypass intermediate files](features.md#bypass-intermediate-files).

When using `pgcopydb` it is possible to achieve both concurrency and streaming
with this simple command line:

```
$ export PGCOPYDB_SOURCE_PGURI="postgres://user@source.host.dev/dbname"
$ export PGCOPYDB_TARGET_PGURI="postgres://role@target.host.dev/dbname"

$ pgcopydb clone --table-jobs 4 --index-jobs 4
```

See the manual page for [pgcopydb clone](ref/pgcopydb_clone.md) for detailed
information about how the command is implemented along with many other supported
options.

## Main pgcopydb features

**Bypass intermediate files**

When using `pg_dump` and `pg_restore` with the `--jobs` option, the table data is
first copied to files on-disk before being read again and sent to the target
server. pgcopydb avoids those steps and instead streams the COPY buffers from
the source to the target with zero processing.

**Use COPY FREEZE**

Postgres has an optimization which reduces post-migration vacuum work by marking
the imported rows as frozen already during the import, that's the FREEZE option
to the VACUUM command. pgcopydb uses that option, unless when using same-table
concurrency.

**Create index concurrency**

When creating an index on a table, Postgres has to implement a full sequential
scan to read all the rows. Implemented in Postgres 8.3 is the
[synchronize_seqscans](https://www.postgresql.org/docs/current/runtime-config-compatible.html#GUC-SYNCHRONIZE-SEQSCANS)
optimization where a single such on-disk read is able to feed several SQL
commands running concurrently in different client sessions.

pgcopydb takes benefit of this feature by running many CREATE INDEX commands on
the same table at the same time. This number is limited by the `--index-jobs`
option.

**Same table concurrency**

When migrating a very large table, it might be beneficial to *partition* the
table and run several COPY commands, distributing the source data using a
non-overlapping WHERE clause. pgcopydb implements that approach with the
`--split-tables-larger-than` option.

**Change Data Capture**

The simplest and safest way to migrate a database to a new Postgres server
requires a maintenance window duration that's dependent on the size of the data
to migrate.

Sometimes the migration context needs to reduce that downtime window. For these
advanced and complex cases, pgcopydb embeds a full replication solution using
the Postgres Logical Decoding low-level APIs.

See the reference manual for the [pgcopydb clone](ref/pgcopydb_clone.md)
`--follow` option.

**Schema restoration error tolerance**

When migrating between PostgreSQL instances with different extension versions,
`pg_restore` may report minor errors that don't affect data integrity. pgcopydb
tolerates these errors, up to `--restore-tolerance` of them, and continues the
migration, logging warnings for verification.

See
[Options for large migrations](operations.md#restore-errors)
for details.

# Features highlights

The `pgcopydb` project was started to allow certain improvements and
considerations which were otherwise not possible to achieve directly with
`pg_dump` and `pg_restore` commands. Below are the details of what `pgcopydb`
can achieve.

## Bypass intermediate files

First aspect is that for `pg_dump` and `pg_restore` to implement concurrency,
they need to write to an intermediate file first.

The [docs for pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html)
say the following about the `--jobs` parameter:

> You can only use this option with the directory output format because this is
> the only output format where multiple processes can write their data at the
> same time.

The
[docs for pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html)
say the following about the `--jobs` parameter:

> Only the custom and directory archive formats are supported with this option.
> The input must be a regular file or directory (not, for example, a pipe or
> standard input).

So the first idea with `pgcopydb` is to provide the `--jobs` concurrency and
bypass intermediate files (and directories) altogether, at least as far as the
actual TABLE DATA set is concerned.

The trick to achieve that is that `pgcopydb` must be able to connect to the
source database during the whole operation, whereas `pg_restore` may be used
from an export on-disk, without having to still be able to connect to the source
database. In the context of `pgcopydb`, requiring access to the source database
is fine. In the context of `pg_restore`, it would not be acceptable.

## Large objects support

The
[Postgres Large-Objects](https://www.postgresql.org/docs/current/largeobjects.html)
API is nobody's favorite, though the trade-offs implemented in that API are
found to be very useful by many application developers. In the context of dump
and restore, Postgres separates the large objects metadata from the large object
contents.

Specifically, the metadata consists of a large-object OID and ACLs, and is
considered to be part of the pre-data section of a Postgres dump.

This means that pgcopydb relies on `pg_dump` to import the large object metadata
from the source to the target Postgres server, but then implements its own logic
to migrate the large objects contents, using several worker processes depending
on the setting of the command-line option `--large-objects-jobs`.

## Concurrency

A major feature of pgcopydb is how concurrency is implemented, including options
to obtain same-table COPY concurrency. See the [Concurrency](concurrency.md)
chapter of the documentation for more information.

## Change Data Capture

pgcopydb implements a full Postgres replication solution based on the
lower-level API for
[Postgres Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding.html).

Always do a test migration first without the `--follow` option to have an idea
of the downtime window needed for your very own case. This will inform your
decision about using the Change Data Capture mode, which makes a migration a lot
more complex to drive to success.

### PostgreSQL logical decoding client

The replication client of pgcopydb has been designed to be able to fetch changes
from the source Postgres instance concurrently to the initial COPY of the data.
Three worker processes are created to handle the logical decoding client:

- The **streaming** process fetches data from the Postgres replication slot
  using the Postgres replication protocol.

- The **transform** process transforms the data fetched from an intermediate
  JSON format into a derivative of the SQL language. In *prefetch mode* this is
  implemented as a batch operation; in *replay mode* this is done in a streaming
  fashion, one line at a time, reading from a unix pipe.

- The **apply** process then applies the SQL script to the target Postgres
  database system and uses Postgres APIs for
  [Replication Progress Tracking](https://www.postgresql.org/docs/current/replication-origins.html).

During the initial COPY phase of operations, pgcopydb follow runs in prefetch
mode and does not apply changes yet. After the initial COPY is done, then the
pgcopydb replication system enters a loop that switches between the following
two modes of operation:

1. In **prefetch mode**, changes are stored to JSON files on-disk, the transform
   process operates on files when a SWITCH occurs, and the apply process
   catches-up with changes on-disk by applying one file at time.

   When the next file to apply does not exist (yet), then the 3 transform worker
   processes stop and the main follow supervisor process then switches to
   *replay mode*.

2. In **replay mode** changes are streamed from the streaming worker process to
   the transform worker process using a Unix PIPE mechanism, and the obtained
   SQL statements are sent to the replay worker process using another Unix PIPE.

   Changes are then replayed in a streaming fashion, end-to-end, with a
   transaction granularity.

### The internal SQL-like script format

The Postgres Logical Decoding API does not provide a CDC format, instead it
allows Postgres extension developers to implement *logical decoding output
plugins*. The Postgres core distribution implements two such output plugins,
[pgoutput](https://www.postgresql.org/docs/current/protocol-logical-replication.html)
and
[test_decoding](https://www.postgresql.org/docs/current/test-decoding.html).
Another commonly used output plugin is named
[wal2json](https://github.com/eulerto/wal2json), which the source server must
install as an extension.

pgcopydb is compatible with the `pgoutput`, `test_decoding` and `wal2json`
plugins. Choose one with the `--plugin` command-line option.

`pgoutput` is the default. It ships with Postgres core, so CDC needs no
extension on the source server, which matters on a managed service where you
cannot install one. It also sends a compact binary protocol: on a mixed
INSERT/UPDATE/DELETE workload it used 4.5x less network volume and about 4x less
source CPU than `wal2json`.

`pgoutput` decodes the tables of a publication rather than the whole database.
Without `--publication`, pgcopydb creates a publication from the table list in
`--filters` and drops it again during
[pgcopydb stream cleanup](ref/pgcopydb_stream.md#pgcopydb-stream-cleanup), so
the source server does the filtering. Creating one needs the `CREATE` privilege
on the database and ownership of every published table. pgcopydb checks both
before it runs the DDL, and when the privileges are missing it reports the
options: grant them, pass an existing publication with `--publication`, or use
`--plugin wal2json`.

The output plugin compatibility means that pgcopydb has to implement code to
parse the output plugin syntax and make sense of it. Internally, the messages
from the output plugin are stored by pgcopydb in a
[JSON Lines](https://jsonlines.org) formatted file, where each line is a JSON
record with decoded metadata about the changes and the output plugin message,
as-is.

This JSON Lines format is transformed into SQL scripts. At first, pgcopydb would
just use SQL for the intermediate format, but then support for
[prepared statements](https://www.postgresql.org/docs/current/sql-prepare.html)
was added as an optimization. This means that our SQL script uses commands such
as the following examples:

```
PREPARE d33a643f AS INSERT INTO public.rental ("rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update") overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7), ($8, $9, $10, $11, $12, $13, $14);
EXECUTE d33a643f["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00","16051","2022-06-01 00:00:00+00","373","293",null,"2","2022-06-01 00:00:00+00"];
```

As you can see in the example, pgcopydb is now able to use a single INSERT
statement with multiple VALUES, which is a huge performance boost. In order to
simplify pgcopydb parsing of the SQL syntax, the choice was made to format the
EXECUTE argument list as a JSON array, which does not comply with the actual SQL
syntax, but is simple and fast to process.

Finally, it's not possible for the transform process to anticipate the actual
session management of the apply process, so SQL statements are always included
with both the PREPARE and the EXECUTE steps. The pgcopydb apply code knows how
to skip PREPARing again, of course.

Unfortunately that means that our SQL files are not actually using SQL syntax
and can't be processed as-is with any SQL client software. At the moment either
using
[pgcopydb stream apply](ref/pgcopydb_stream.md#pgcopydb-stream-apply) or writing
your own processing code is required.

## Internal catalogs (SQLite)

To be able to implement pgcopydb operations, a list of SQL objects such as
tables, indexes, constraints and sequences is needed internally. While pgcopydb
used to handle such a list as an array in-memory, with also a hash-table for
direct lookup (by oid and by *restore list name*), in some cases the source
database contains so many objects that these arrays do not fit in memory.

As pgcopydb is written in C, the current best approach to handle an array of
objects that needs to spill to disk and supports direct lookup is actually the
SQLite library, file format, and embedded database engine.

That's why the current version of pgcopydb uses SQLite to handle its catalogs.

Internally pgcopydb stores metadata information in three different catalogs, all
found in the `${TMPDIR}/pgcopydb/schema/` directory by default, unless using the
recommended `--dir` option.

- The **source** catalog registers metadata about the source database, and also
  some metadata about the pgcopydb context, consistency, and progress.

- The **filters** catalog is only used when the `--filters` option is provided,
  and it registers metadata about the objects in the source database that are
  going to be skipped.

  This is necessary because the filtering is implemented using the
  `pg_restore --list` and `pg_restore --use-list` options. The Postgres archive
  Table Of Contents format contains an object OID and its *restore list name*,
  and pgcopydb needs to be able to lookup for that OID or name in its filtering
  catalogs.

- The **target** catalog registers metadata about the target database, such as
  the list of roles, the list of schemas, or the list of already existing
  constraints found on the target database.

## Schema restoration error tolerance

When migrating databases between PostgreSQL instances with different versions or
extension versions, `pg_restore` may encounter minor errors that don't affect
the integrity of the migration. A common example is PostgreSQL extension version
mismatches.

In these cases, `pg_restore` exits with code 1 but reports "errors ignored on
restore: N" in its output. These errors are typically benign: the schema objects
that couldn't be restored already exist or are incompatible in ways that don't
affect the data migration.

pgcopydb tolerates such errors automatically. When `pg_restore` exits with
code 1:

1. pgcopydb parses the output for "errors ignored on restore: N"
2. If N is at most the tolerance, pgcopydb logs a warning and continues
3. If N is above the tolerance, or N = 0, pgcopydb fails as before to prevent
   data corruption

This behavior allows migrations to proceed through:

- Extension version mismatches between source and target
- Custom type definitions that differ slightly between PostgreSQL versions
- Minor schema compatibility issues that don't affect data integrity

**Configuration**: the tolerance defaults to 10 and is set with
`--restore-tolerance`. See
[Options for large migrations](operations.md#restore-errors).

**Warning logs**: when errors are tolerated, pgcopydb logs:

```
WARN: pg_restore exited with code 1 but reported 2 ignored errors (within tolerance of 10)
WARN: Continuing despite pg_restore errors - please verify schema integrity
```

After migration, verify that your schema is correct and that the ignored errors
were indeed benign by checking the application behavior and running schema
comparison tools.

## Visibility and failure reporting

pgcopydb provides detailed visibility into database objects and comprehensive
failure reporting to help diagnose and resolve migration issues quickly.

### Listing views and triggers

In addition to tables, indexes, and sequences, pgcopydb can list views and
triggers from the source database:

```
$ pgcopydb list views --source <connection-string>
     OID |                    Schema Name |                      View Name
---------+--------------------------------+-------------------------------
   17090 |                         public |                     actor_info
   17119 |                         public |                  customer_list

$ pgcopydb list triggers --source <connection-string>
     OID |              Trigger Name |               Schema Name |                Table Name |  Table OID
---------+---------------------------+---------------------------+---------------------------+-----------
   17306 |              last_updated |                    public |                  customer |      17043
   17301 |              last_updated |                    public |                     actor |      17055
```

These commands are useful for:

- **Pre-migration assessment**: understanding the full scope of database objects
  before starting a migration
- **Verification**: confirming which views and triggers will be included in the
  migration
- **Documentation**: generating an inventory of database objects

Views and triggers are tracked in pgcopydb's internal SQLite catalogs and their
counts are included in summary reports.

### Failure reports

When a migration fails, pgcopydb provides a detailed summary showing exactly
what completed and what didn't. This report includes:

- **Completed phases** with their durations
- **Failure location** showing which phase failed
- **Progress within the failed phase**
- **Resource summary** showing counts of all database objects: tables copied,
  indexes created, constraints applied, sequences found, views found, triggers
  found
- **Resume instructions** suggesting the `--resume` flag for continuing the
  migration

Example failure report:

```
Migration Failed
================

Completed Phases:
  COPY Catalog Queries - 5s
  COPY Dump Schema - 10s
  COPY Prepare Schema (pre-data) - 15s
  COPY Data - 120s

Failed at: CREATE INDEX
  Progress: 50/100 indexes created

Resource Summary:
  Tables:      100/100
  Indexes:     50/100
  Constraints: 0/100
  Sequences:   13 found
  Views:       7 found
  Triggers:    15 found

To resume, use: pgcopydb clone --resume
```

This reporting helps identify:

- **Partial success**: what work has already been completed and doesn't need to
  be repeated
- **Failure context**: exactly where and when the migration failed
- **Resource scope**: the total number of objects that need to be migrated
- **Next steps**: clear guidance on how to resume or retry the migration

Failure reports are automatically displayed when migrations fail during data
copy, index creation, or schema finalization phases.

# pgcopydb

pgcopydb - copy an entire Postgres database from source to target

## Synopsis

pgcopydb provides the following commands:

<!-- BEGIN HELP: pgcopydb -->
```
pgcopydb: pgcopydb tool
usage: pgcopydb [ --verbose --quiet ]


Available commands:
  pgcopydb
    clone     Clone an entire database from source to target
    fork      Clone an entire database from source to target
    follow    Replay changes from the source database to the target database
    snapshot  Create and export a snapshot on the source database
  + compare   Compare source and target databases
  + copy      Implement the data section of the database copy
  + dump      Dump database objects from a Postgres instance
  + restore   Restore database objects into a Postgres instance
  + list      List database objects from a Postgres instance
  + stream    Stream changes from the source database
    ping      Attempt to connect to the source and target instances
    help      Print help message
    version   Print pgcopydb version
```
<!-- END HELP -->

## Description

The pgcopydb command implements a full migration of an entire Postgres
database from a source instance to a target instance. Both the Postgres
instances must be available for the entire duration of the command.

The pgcopydb command also implements a full
[Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding.html)
client for Postgres, allowing Change Data Capture to replay data changes (DML)
happening on the source database after the base copy snapshot. The pgcopydb
logical decoding client works with the
[pgoutput](https://www.postgresql.org/docs/current/protocol-logical-replication.html),
[test_decoding](https://www.postgresql.org/docs/current/test-decoding.html) and
[wal2json](https://github.com/eulerto/wal2json/) output plugins, and defaults to
pgoutput.

## pgcopydb help

The `pgcopydb help` command lists all the supported sub-commands:

<!-- BEGIN HELP: pgcopydb help -->
```
  pgcopydb
    clone     Clone an entire database from source to target
    fork      Clone an entire database from source to target
    follow    Replay changes from the source database to the target database
    snapshot  Create and export a snapshot on the source database
  + compare   Compare source and target databases
  + copy      Implement the data section of the database copy
  + dump      Dump database objects from a Postgres instance
  + restore   Restore database objects into a Postgres instance
  + list      List database objects from a Postgres instance
  + stream    Stream changes from the source database
    ping      Attempt to connect to the source and target instances
    help      Print help message
    version   Print pgcopydb version

  pgcopydb compare
    schema  Compare source and target schema
    data    Compare source and target data

  pgcopydb copy
    db           Copy an entire database from source to target
    roles        Copy the roles from the source instance to the target instance
    extensions   Copy the extensions from the source instance to the target instance
    schema       Copy the database schema from source to target
    data         Copy the data section from source to target
    table-data   Copy the data from all tables in database from source to target
    blobs        Copy the blob data from the source database to the target
    sequences    Copy the current value from all sequences in database from source to target
    indexes      Create all the indexes found in the source database in the target
    constraints  Create all the constraints found in the source database in the target

  pgcopydb dump
    schema  Dump source database schema as custom files in work directory
    roles   Dump source database roles as custome file in work directory

  pgcopydb restore
    schema      Restore a database schema from custom files to target database
    pre-data    Restore a database pre-data schema from custom file to target database
    post-data   Restore a database post-data schema from custom file to target database
    roles       Restore database roles from SQL file to target database
    parse-list  Parse pg_restore --list output from custom file

  pgcopydb list
    databases    List databases
    extensions   List all the source extensions to copy
    collations   List all the source collations to copy
    tables       List all the source tables to copy data from
    table-parts  List a source table copy partitions
    sequences    List all the source sequences to copy data from
    views        List all the source views
    triggers     List all the source triggers
    indexes      List all the indexes to create again after copying the data
    depends      List all the dependencies to filter-out
    schema       List the schema to migrate, formatted in JSON
    progress     List the progress

  pgcopydb stream
    setup      Setup source and target systems for logical decoding
    cleanup    Cleanup source and target systems for logical decoding
    prefetch   Stream JSON changes from the source database and transform them to SQL
    catchup    Apply prefetched changes from SQL files to the target database
    replay     Replay changes from the source to the target database, live
  + sentinel   Maintain a sentinel table
    receive    Stream changes from the source database
    transform  Transform changes from the source database into SQL commands
    apply      Apply changes from the source database into the target database

  pgcopydb stream sentinel
    setup  Setup the sentinel table
    get    Get the sentinel table values
  + set    Set the sentinel table values

  pgcopydb stream sentinel set
    startpos  Set the sentinel start position LSN
    endpos    Set the sentinel end position LSN
    apply     Set the sentinel apply mode
    prefetch  Set the sentinel prefetch mode
```
<!-- END HELP -->

## pgcopydb version

The `pgcopydb version` command outputs the version string of the version of
pgcopydb used, and can do that in the JSON format when using the `--json`
option.

```
$ pgcopydb version
pgcopydb version 0.19.0
compiled with PostgreSQL 18.0 (Debian 18.0-1.pgdg12+3) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
compatible with Postgres 10, 11, 12, 13, 14, 15, 16, 17, and 18
```

In JSON:

```
$ pgcopydb version --json
{
    "pgcopydb": "0.19.0",
    "pg_major": "18",
    "pg_version": "18.0 (Debian 18.0-1.pgdg12+3)",
    "pg_version_str": "PostgreSQL 18.0 (Debian 18.0-1.pgdg12+3) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit",
    "pg_version_num": 180000
}
```

The details about the Postgres version apply to the version that was used to
build pgcopydb from sources, so that is the version of the client library
`libpq`.

## pgcopydb ping

The `pgcopydb ping` command attempts to connect to both the source and the
target Postgres databases, concurrently.

<!-- BEGIN HELP: pgcopydb ping -->
```
pgcopydb ping: Attempt to connect to the source and target instances
usage: pgcopydb ping  --source ... --target ... 

  --source              Postgres URI to the source database
  --target              Postgres URI to the target database
```
<!-- END HELP -->

An example output looks like the following:

```
$ pgcopydb ping
18:04:48 84679 INFO   Running pgcopydb version 0.19.0
18:04:48 84683 INFO   Successfully could connect to target database at "postgres://@:/plop?"
18:04:48 84682 INFO   Successfully could connect to source database at "postgres://@:/pagila?"
```

This command implements a retry policy (named *Decorrelated Jitter*) and can be
used in automation to make sure that the databases are ready to accept
connections.

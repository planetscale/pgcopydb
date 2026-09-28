# pgcopydb list

pgcopydb list - List database objects from a Postgres instance

This command prefixes the following sub-commands:

<!-- BEGIN HELP: pgcopydb list -->
```
pgcopydb list: List database objects from a Postgres instance

Available commands:
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
```
<!-- END HELP -->

## pgcopydb list databases

pgcopydb list databases - List databases

The command `pgcopydb list databases` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
databases there.

<!-- BEGIN HELP: pgcopydb list databases -->
```
pgcopydb list databases: List databases
usage: pgcopydb list databases  --source ... 

  --source            Postgres URI to the source database
```
<!-- END HELP -->

## pgcopydb list extensions

pgcopydb list extensions - List all the source extensions to copy

The command `pgcopydb list extensions` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
extensions to COPY to the target database.

<!-- BEGIN HELP: pgcopydb list extensions -->
```
pgcopydb list extensions: List all the source extensions to copy
usage: pgcopydb list extensions  --source ... 

  --source              Postgres URI to the source database
  --json                Format the output using JSON
  --available-versions  List available extension versions
  --requirements        List extensions requirements
```
<!-- END HELP -->

The command `pgcopydb list extensions --available-versions` is typically used
with the target database. If you're using the connection string environment
variables, that looks like the following:

```
$ pgcopydb list extensions --available-versions --source ${PGCOPYDB_TARGET_PGURI}
```

## pgcopydb list collations

pgcopydb list collations - List all the source collations to copy

The command `pgcopydb list collations` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
collations to COPY to the target database.

<!-- BEGIN HELP: pgcopydb list collations -->
```
pgcopydb list collations: List all the source collations to copy
usage: pgcopydb list collations  --source ... 

  --source            Postgres URI to the source database
```
<!-- END HELP -->

The SQL query that is used lists the database collation, and then any
non-default collation that's used in a user column or a user index.

## pgcopydb list tables

pgcopydb list tables - List all the source tables to copy data from

The command `pgcopydb list tables` connects to the source database and executes
a SQL query using the Postgres catalogs to get a list of all the tables to COPY
the data from.

<!-- BEGIN HELP: pgcopydb list tables -->
```
pgcopydb list tables: List all the source tables to copy data from
usage: pgcopydb list tables  --source ... 

  --source            Postgres URI to the source database
  --filter <filename> Use the filters defined in <filename>
  --force             Force fetching catalogs again
  --list-skipped      List only tables that are setup to be skipped
  --without-pkey      List only tables that have no primary key
```
<!-- END HELP -->

## pgcopydb list table-parts

pgcopydb list table-parts - List a source table copy partitions

The command `pgcopydb list table-parts` connects to the source database and
executes a SQL query using the Postgres catalogs to get detailed information
about the given source table, and then another SQL query to compute how to split
this source table given the size threshold argument.

<!-- BEGIN HELP: pgcopydb list table-parts -->
```
pgcopydb list table-parts: List a source table copy partitions
usage: pgcopydb list table-parts  --source ... 

  --source                    Postgres URI to the source database
  --force                     Force fetching catalogs again
  --schema-name               Name of the schema where to find the table
  --table-name                Name of the target table
  --split-tables-larger-than  Size threshold to consider partitioning
  --split-max-parts           Maximum number of jobs for Same-table concurrency 
  --skip-split-by-ctid        Skip the ctid split
  --estimate-table-sizes      Allow using estimates for relation sizes
```
<!-- END HELP -->

## pgcopydb list sequences

pgcopydb list sequences - List all the source sequences to copy data from

The command `pgcopydb list sequences` connects to the source database and
executes a SQL query using the Postgres catalogs to get a list of all the
sequences to COPY the data from.

<!-- BEGIN HELP: pgcopydb list sequences -->
```
pgcopydb list sequences: List all the source sequences to copy data from
usage: pgcopydb list sequences  --source ... 

  --source            Postgres URI to the source database
  --force             Force fetching catalogs again
  --filter <filename> Use the filters defined in <filename>
  --list-skipped      List only tables that are setup to be skipped
```
<!-- END HELP -->

## pgcopydb list indexes

pgcopydb list indexes - List all the indexes to create again after copying the
data

The command `pgcopydb list indexes` connects to the source database and executes
a SQL query using the Postgres catalogs to get a list of all the indexes to COPY
the data from.

<!-- BEGIN HELP: pgcopydb list indexes -->
```
pgcopydb list indexes: List all the indexes to create again after copying the data
usage: pgcopydb list indexes  --source ... [ --schema-name [ --table-name ] ]

  --source            Postgres URI to the source database
  --force             Force fetching catalogs again
  --schema-name       Name of the schema where to find the table
  --table-name        Name of the target table
  --filter <filename> Use the filters defined in <filename>
  --list-skipped      List only tables that are setup to be skipped
```
<!-- END HELP -->

## pgcopydb list depends

pgcopydb list depends - List all the dependencies to filter-out

The command `pgcopydb list depends` connects to the source database and executes
a SQL query using the Postgres catalogs to get a list of all the objects that
depend on excluded objects from the filtering rules.

<!-- BEGIN HELP: pgcopydb list depends -->
```
pgcopydb list depends: List all the dependencies to filter-out
usage: pgcopydb list depends  --source ... [ --schema-name [ --table-name ] ]

  --source            Postgres URI to the source database
  --force             Force fetching catalogs again
  --schema-name       Name of the schema where to find the table
  --table-name        Name of the target table
  --filter <filename> Use the filters defined in <filename>
  --list-skipped      List only tables that are setup to be skipped
```
<!-- END HELP -->

## pgcopydb list schema

pgcopydb list schema - List the schema to migrate, formatted in JSON

The command `pgcopydb list schema` connects to the source database and executes
a SQL queries using the Postgres catalogs to get a list of the tables, indexes,
and sequences to migrate. The command then outputs a JSON formatted string that
contains detailed information about all those objects.

<!-- BEGIN HELP: pgcopydb list schema -->
```
pgcopydb list schema: List the schema to migrate, formatted in JSON
usage: pgcopydb list schema  --source ... 

  --source            Postgres URI to the source database
  --force             Force fetching catalogs again
  --filter <filename> Use the filters defined in <filename>
```
<!-- END HELP -->

## pgcopydb list progress

pgcopydb list progress - List the progress

The command `pgcopydb list progress` reads the internal SQLite catalogs in the
work directory, parses it, and then computes how many tables and indexes are
planned to be copied and created on the target database, how many have been done
already, and how many are in-progress.

The `--summary` option displays the top-level summary, and can be used while the
command is running or after-the-fact.

When using the option `--json` the JSON formatted output also includes a list of
all the tables and indexes that are currently being processed.

<!-- BEGIN HELP: pgcopydb list progress -->
```
pgcopydb list progress: List the progress
usage: pgcopydb list progress  --source ... 

  --source  Postgres URI to the source database
  --summary List the summary, requires --json
  --json    Format the output using JSON
  --dir     Work directory to use
```
<!-- END HELP -->

## Options

The following options are available to `pgcopydb list` sub-commands:

`--source`

Connection string to the source Postgres instance. See the Postgres
documentation for
[connection strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
for the details. In short both the quoted form `"host=... dbname=..."` and the
URI form `postgres://user@host:5432/dbname` are supported.

`--schema-name`

Filter indexes from a given schema only.

`--table-name`

Filter indexes from a given table only (use `--schema-name` to fully qualify the
table).

`--without-pkey`

List only tables from the source database when they have no primary key attached
to their schema.

`--filter <filename>`

This option allows to skip objects in the list operations. See
[Filtering](pgcopydb_config.md#filtering) for details about the expected file
format and the filtering options available.

`--list-skipped`

Instead of listing objects that are selected for copy by the filters installed
with the `--filter` option, list the objects that are going to be skipped when
using the filters.

`--summary`

Instead of listing current progress when the command is still running, instead
list the summary with timing details for each step and for all tables, indexes,
and constraints.

`--json`

The output of the command is formatted in JSON, when supported. Ignored
otherwise.

`--verbose`

Increase current verbosity. The default level of verbosity is INFO. In ascending
order pgcopydb knows about the following verbosity levels: FATAL, ERROR, WARN,
INFO, NOTICE, DEBUG, TRACE.

`--debug`

Set current verbosity to DEBUG level.

`--trace`

Set current verbosity to TRACE level.

`--quiet`

Set current verbosity to ERROR level.

## Environment

`PGCOPYDB_SOURCE_PGURI`

Connection string to the source Postgres instance. When `--source` is omitted
from the command line, then this environment variable is used.

## Examples

Listing the tables:

```
$ pgcopydb list tables
14:35:18 13827 INFO  Listing ordinary tables in "port=54311 host=localhost dbname=pgloader"
14:35:19 13827 INFO  Fetched information for 56 tables
     OID |          Schema Name |           Table Name |  Est. Row Count |    On-disk size
---------+----------------------+----------------------+-----------------+----------------
   17085 |                  csv |                track |            3503 |          544 kB
   17098 |             expected |                track |            3503 |          544 kB
   17290 |             expected |           track_full |            3503 |          544 kB
   17276 |               public |           track_full |            3503 |          544 kB
   17016 |             expected |            districts |             440 |           72 kB
   17007 |               public |            districts |             440 |           72 kB
   16998 |                  csv |               blocks |             460 |           48 kB
   17003 |             expected |               blocks |             460 |           48 kB
   17405 |                  csv |              partial |               7 |           16 kB
   17323 |                  err |               errors |               0 |           16 kB
```

Listing a table list of COPY partitions:

```
$ pgcopydb list table-parts --table-name rental --split-at 300kB
16:43:26 73794 INFO  Running pgcopydb version 0.19.0
16:43:26 73794 INFO  Listing COPY partitions for table "public"."rental" in "postgres://@:/pagila?"
16:43:26 73794 INFO  Table "public"."rental" COPY will be split 5-ways
      Part |        Min |        Max |      Count
-----------+------------+------------+-----------
       1/5 |          1 |       3211 |       3211
       2/5 |       3212 |       6422 |       3211
       3/5 |       6423 |       9633 |       3211
       4/5 |       9634 |      12844 |       3211
       5/5 |      12845 |      16049 |       3205
```

Listing the indexes:

```
$ pgcopydb list indexes
14:35:07 13668 INFO  Listing indexes in "port=54311 host=localhost dbname=pgloader"
14:35:07 13668 INFO  Fetching all indexes in source database
14:35:07 13668 INFO  Fetched information for 12 indexes
     OID |     Schema |           Index Name |         conname |                Constraint | DDL
---------+------------+----------------------+-----------------+---------------------------+---------------------
   17002 |        csv |      blocks_ip4r_idx |                 |                           | CREATE INDEX blocks_ip4r_idx ON csv.blocks USING gist (iprange)
   17415 |        csv |        partial_b_idx |                 |                           | CREATE INDEX partial_b_idx ON csv.partial USING btree (b)
   17414 |        csv |        partial_a_key |   partial_a_key |                UNIQUE (a) | CREATE UNIQUE INDEX partial_a_key ON csv.partial USING btree (a)
   17092 |        csv |           track_pkey |      track_pkey |     PRIMARY KEY (trackid) | CREATE UNIQUE INDEX track_pkey ON csv.track USING btree (trackid)
   17329 |        err |          errors_pkey |     errors_pkey |           PRIMARY KEY (a) | CREATE UNIQUE INDEX errors_pkey ON err.errors USING btree (a)
```

Listing the schema in JSON:

```
$ pgcopydb list schema --split-at 200kB
```

This gives a JSON document describing every table, index and sequence that the
migration covers. The table entries look like this:

```json
{
  "setup": {
    "snapshot": "00000003-00000048-1",
    "source_pguri": "postgres:\/\/@:\/pagila?",
    "target_pguri": "postgres:\/\/@:\/plop?",
    "table-jobs": 4,
    "index-jobs": 4,
    "split-tables-larger-than": 204800
  },
  "tables": [
    {
      "oid": 317972,
      "schema": "public",
      "name": "rental",
      "reltuples": 16044,
      "bytes": 1253376,
      "bytes-pretty": "1224 kB",
      "exclude-data": false,
      "restore-list-name": "public rental postgres",
      "part-key": "rental_id",
      "parts": [
        {
          "number": 1,
          "total": 7,
          "min": 1,
          "max": 2294,
          "count": 2294
        }
      ]
    }
  ],
  "indexes": [
    {
      "oid": 378130,
      "schema": "public",
      "name": "idx_store_id_film_id",
      "isPrimary": false,
      "isUnique": false,
      "columns": "store_id film_id",
      "sql": "CREATE INDEX idx_store_id_film_id ON public.inventory USING btree (store_id, film_id)",
      "restore-list-name": "public idx_store_id_film_id postgres",
      "table": {
        "oid": 317980,
        "schema": "public",
        "name": "inventory"
      }
    }
  ],
  "sequences": [
    {
      "oid": 317911,
      "schema": "public",
      "name": "actor_actor_id_seq",
      "last-value": 200,
      "is-called": true,
      "restore-list-name": "public actor_actor_id_seq postgres"
    }
  ]
}
```

Listing current progress (log lines removed):

```
$ pgcopydb list progress 2>/dev/null
             |  Total Count |  In Progress |         Done
-------------+--------------+--------------+-------------
      Tables |           21 |            4 |            7
     Indexes |           48 |           14 |            7
```

Listing current progress, in JSON:

```json
{
    "table-jobs": 4,
    "index-jobs": 4,
    "tables": {
        "total": 21,
        "done": 9,
        "in-progress": [
            {
                "oid": 317908,
                "schema": "public",
                "name": "payment_p2020_01",
                "reltuples": 1157,
                "bytes": 98304,
                "bytes-pretty": "96 kB",
                "exclude-data": false,
                "restore-list-name": "public payment_p2020_01 postgres",
                "part-key": "",
                "process": {
                    "pid": 75159,
                    "start-time-epoch": 1662476249,
                    "start-time-string": "2026-09-06 16:57:29 CEST",
                    "command": "COPY \"public\".\"payment_p2020_01\""
                }
            }
        ]
    },
    "indexes": {
        "total": 48,
        "done": 39,
        "in-progress": [
            {
                "oid": 378283,
                "schema": "pgcopydb",
                "name": "sentinel_expr_idx",
                "isPrimary": false,
                "isUnique": true,
                "columns": "",
                "sql": "CREATE UNIQUE INDEX sentinel_expr_idx ON pgcopydb.sentinel USING btree ((1))",
                "restore-list-name": "pgcopydb sentinel_expr_idx dim",
                "table": {
                    "oid": 378280,
                    "schema": "pgcopydb",
                    "name": "sentinel"
                },
                "process": {
                    "pid": 74372,
                    "start-time-epoch": 1662476080,
                    "start-time-string": "2026-09-06 16:54:40 CEST"
                }
            }
        ]
    }
}
```

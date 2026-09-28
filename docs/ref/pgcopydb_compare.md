# pgcopydb compare

pgcopydb compare - Compare source and target databases

The command `pgcopydb compare` connects to the source and target databases and
executes SQL queries to get Postgres catalog information about the table,
indexes and sequences that are migrated.

The tool then compares either the schema definitions or the data contents of the
selected tables, and report success by means of an Unix return code of zero.

At the moment, the `pgcopydb compare` tool is pretty limited in terms of schema
support: it only covers what pgcopydb needs to know about the database schema,
which isn't much.

<!-- BEGIN HELP: pgcopydb compare -->
```
pgcopydb compare: Compare source and target databases

Available commands:
  pgcopydb compare
    schema  Compare source and target schema
    data    Compare source and target data
```
<!-- END HELP -->

## pgcopydb compare schema

pgcopydb compare schema - Compare source and target schema

The command `pgcopydb compare schema` connects to the source and target
databases and executes SQL queries using the Postgres catalogs to get a list of
tables, indexes, constraints and sequences there.

<!-- BEGIN HELP: pgcopydb compare schema -->
```
pgcopydb compare schema: Compare source and target schema
usage: pgcopydb compare schema  --source ... 

  --source         Postgres URI to the source database
  --target         Postgres URI to the target database
  --dir            Work directory to use
```
<!-- END HELP -->

## pgcopydb compare data

pgcopydb compare data - Compare source and target data

The command `pgcopydb compare data` connects to the source and target databases
and executes SQL queries using the Postgres catalogs to get a list of tables,
indexes, constraints and sequences there.

Then it uses a SQL query with the following template to compute the row count
and a checksum for each table:

```sql
/*
 * Compute the hashtext of every single row in the table, and aggregate the
 * results as a sum of bigint numbers. Because the sum of bigint could
 * overflow to numeric, the aggregated sum is then hashed into an MD5
 * value: bigint is 64 bits, MD5 is 128 bits.
 *
 * Also, to lower the chances of a collision, include the row count in the
 * computation of the MD5 by appending it to the input string of the MD5
 * function.
 */
select count(1) as cnt,
       md5(
         format(
           '%%s-%%s',
           sum(hashtext(__COLS__::text)::bigint),
           count(1)
         )
       )::uuid as chksum
from only __TABLE__
```

Running such a query on a large table can take a lot of time.

<!-- BEGIN HELP: pgcopydb compare data -->
```
pgcopydb compare data: Compare source and target data
usage: pgcopydb compare data  --source ... 

  --source         Postgres URI to the source database
  --target         Postgres URI to the target database
  --dir            Work directory to use
  --json           Format the output using JSON
```
<!-- END HELP -->

## Options

The following options are available to `pgcopydb compare schema` and
`pgcopydb compare data` subcommands:

`--source`

Connection string to the source Postgres instance. See the Postgres
documentation for
[connection strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
for the details. In short both the quoted form `"host=... dbname=..."` and the
URI form `postgres://user@host:5432/dbname` are supported.

`--target`

Connection string to the target Postgres instance.

`--dir`

During its normal operations pgcopydb creates a lot of temporary files to track
sub-processes progress. Temporary files are created in the directory specified
by this option, or defaults to `${TMPDIR}/pgcopydb` when the environment
variable is set, or otherwise to `/tmp/pgcopydb`.

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

`PGCOPYDB_TARGET_PGURI`

Connection string to the target Postgres instance. When `--target` is omitted
from the command line, then this environment variable is used.

## Examples

Comparing pgcopydb limited understanding of the schema:

```
$ pgcopydb compare schema --notice
INFO   Running pgcopydb version 0.19.0
NOTICE Using work dir "/tmp/pgcopydb"
NOTICE Work directory "/tmp/pgcopydb" already exists
INFO   A previous run has run through completion
INFO   SOURCE: Connecting to "postgres:///pagila"
INFO   Fetched information for 1 extensions
INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
INFO   Fetched information for 49 indexes
INFO   Fetching information for 16 sequences
NOTICE Skipping target catalog preparation
NOTICE Storing migration schema in JSON file "/tmp/pgcopydb/compare/source-schema.json"
INFO   TARGET: Connecting to "postgres:///plop"
INFO   Fetched information for 6 extensions
INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
INFO   Fetched information for 49 indexes
INFO   Fetching information for 16 sequences
NOTICE Skipping target catalog preparation
NOTICE Storing migration schema in JSON file "/tmp/pgcopydb/compare/target-schema.json"
INFO   [SOURCE] table: 25 index: 49 sequence: 16
INFO   [TARGET] table: 25 index: 49 sequence: 16
NOTICE Matched table "public"."rental": 7 columns ok, 3 indexes ok
NOTICE Matched table "public"."film": 14 columns ok, 5 indexes ok
NOTICE Matched table "public"."inventory": 4 columns ok, 2 indexes ok
NOTICE Matched table "public"."customer": 10 columns ok, 4 indexes ok
NOTICE Matched sequence "public"."actor_actor_id_seq" (last value 200)
NOTICE Matched sequence "public"."film_film_id_seq" (last value 1000)
NOTICE Matched sequence "public"."rental_rental_id_seq" (last value 16053)
INFO   pgcopydb schema inspection is successful
```

Comparing data:

```
$ pgcopydb compare data
INFO   A previous run has run through completion
INFO   SOURCE: Connecting to "postgres:///pagila"
INFO   Fetched information for 1 extensions
INFO   Fetched information for 25 tables, with an estimated total of 5179  tuples and 190 MB
INFO   Fetched information for 49 indexes
INFO   Fetching information for 16 sequences
INFO   TARGET: Connecting to "postgres:///plop"
INFO   Fetched information for 6 extensions
INFO   Fetched information for 25 tables, with an estimated total of 5219  tuples and 190 MB
INFO   Fetched information for 49 indexes
INFO   Fetching information for 16 sequences
INFO   Comparing data for 25 tables
ERROR  Table "public"."test" has 5173526 rows on source, 5173525 rows on target
ERROR  Table "public"."test" has checksum be66f291-2774-9365-400c-1ccd5160bdf on source, 8be89afa-bceb-f501-dc7b-0538dc17fa3 on target
ERROR  Table "public"."foo" has 3 rows on source, 2 rows on target
ERROR  Table "public"."foo" has checksum a244eba3-376b-75e6-6720-e853b485ef6 on source, 594ae64d-2216-f687-2f11-45cbd9c7153 on target
                    Table Name | ! |                      Source Checksum |                      Target Checksum
-------------------------------+---+--------------------------------------+-------------------------------------
               "public"."test" | ! |  be66f291-2774-9365-400c-1ccd5160bdf |  8be89afa-bceb-f501-dc7b-0538dc17fa3
             "public"."rental" |   |  e7dfabf3-baa8-473a-8fd3-76d59e56467 |  e7dfabf3-baa8-473a-8fd3-76d59e56467
               "public"."film" |   |  c5058d1e-aaf4-f058-6f1e-76d5db63da9 |  c5058d1e-aaf4-f058-6f1e-76d5db63da9
          "public"."inventory" |   |  72f9afd8-0064-3642-acd7-9ee1f444efe |  72f9afd8-0064-3642-acd7-9ee1f444efe
           "public"."customer" |   |  11973c6a-6df3-c502-5495-64f42e0386c |  11973c6a-6df3-c502-5495-64f42e0386c
                "public"."foo" | ! |  a244eba3-376b-75e6-6720-e853b485ef6 |  594ae64d-2216-f687-2f11-45cbd9c7153
              "public"."store" |   |  d8477e63-0661-90a4-03fa-fcc26a95865 |  d8477e63-0661-90a4-03fa-fcc26a95865
```

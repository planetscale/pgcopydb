# pgcopydb clone

The main pgcopydb operation is the clone operation, and for historical and user
friendliness reasons two aliases are available that implement the same
operation:

```
pgcopydb
  clone     Clone an entire database from source to target
  fork      Clone an entire database from source to target
```

## Synopsis

The command `pgcopydb clone` copies a database from the given source Postgres
instance to the target Postgres instance.

<!-- BEGIN HELP: pgcopydb clone -->
```
pgcopydb clone: Clone an entire database from source to target
usage: pgcopydb clone  --source ... --target ... [ --table-jobs ... --index-jobs ... ] 

  --source                      Postgres URI to the source database
  --target                      Postgres URI to the target database
  --dir                         Work directory to use
  --table-jobs                  Number of concurrent COPY jobs to run
  --index-jobs                  Number of concurrent CREATE INDEX jobs to run
  --restore-jobs                Number of concurrent jobs for pg_restore
  --restore-tolerance           Max pg_restore errors to tolerate (default 10)
  --large-objects-jobs          Number of concurrent Large Objects jobs to run
  --split-tables-larger-than    Same-table concurrency size threshold
  --split-max-parts             Maximum number of jobs for Same-table concurrency 
  --estimate-table-sizes        Allow using estimates for relation sizes
  --drop-if-exists              On the target database, clean-up from a previous run first
  --roles                       Also copy roles found on source to target
  --no-role-passwords           Do not dump passwords for roles
  --no-owner                    Do not set ownership of objects to match the original database
  --no-acl                      Prevent restoration of access privileges (grant/revoke commands).
  --no-comments                 Do not output commands to restore comments
  --no-tablespaces              Do not output commands to select tablespaces
  --skip-large-objects          Skip copying large objects (blobs)
  --skip-extensions             Skip restoring extensions
  --skip-ext-comments           Skip restoring COMMENT ON EXTENSION
  --skip-collations             Skip restoring collations
  --skip-publications           Skip restoring publications
  --skip-vacuum                 Skip running VACUUM ANALYZE
  --skip-analyze                Skip running vacuumdb --analyze-only
  --skip-db-properties          Skip copying ALTER DATABASE SET properties
  --skip-split-by-ctid          Skip spliting tables by ctid
  --skip-xid-check             Skip the XID wraparound proximity check
  --requirements <filename>     List extensions requirements
  --filters <filename>          Use the filters defined in <filename>
  --fail-fast                   Abort early in case of error
  --restart                     Allow restarting when temp files exist already
  --resume                      Allow resuming operations after a failure
  --not-consistent              Allow taking a new snapshot on the source database
  --snapshot                    Use snapshot obtained with pg_export_snapshot
  --follow                      Implement logical decoding to replay changes
  --plugin                      Output plugin to use (pgoutput, test_decoding, wal2json)
  --publication                 Publication to use with the pgoutput plugin
  --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
  --slot-name                   Use this Postgres replication slot name
  --create-slot                 Create the replication slot
  --origin                      Use this Postgres replication origin node name
  --endpos                      Stop replaying changes when reaching this LSN
  --defer-indexes               Defer index building until after all table data is copied
  --defer-analyze               Defer ANALYZE until after post-data restore
  --defer-validate-fks          Create FK constraints as NOT VALID, skipping validation scan
  --use-copy-binary             Use the COPY BINARY format for COPY operations
  --prune-threshold           Max size of applied CDC files to retain (e.g. 10GB, 0 to disable)
  --prune-min-age             Min age before applied CDC files can be deleted (e.g. 15m, 2h)
```
<!-- END HELP -->

## pgcopydb fork

The command `pgcopydb fork` copies a database from the given source Postgres
instance to the target Postgres instance. This command is an alias to the
command `pgcopydb clone` seen above.

## Description

The `pgcopydb clone` command implements both a base copy of a source database
into a target database and also a full
[Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding.html)
client.

### Base copy, or the clone operation

The `pgcopydb clone` command implements the following steps:

1. `pgcopydb` gets the list of ordinary and partitioned tables from a catalog
   query on the source database, and also the list of indexes, and the list of
   sequences with their current values.

   When filtering is used, the list of objects OIDs that are meant to be
   filtered out is built during this step.

2. `pgcopydb` calls into `pg_dump` to produce the `pre-data` section and the
   `post-data` sections of the dump using Postgres custom format.

3. The `pre-data` section of the dump is restored on the target database using
   the `pg_restore` command, creating all the Postgres objects from the source
   database into the target database.

   When filtering is used, the `pg_restore --use-list` feature is used to filter
   the list of objects to restore in this step.

   This step uses as many as `--restore-jobs` jobs for `pg_restore` to share the
   workload and restore the objects in parallel.

   > **Note**
   >
   > pgcopydb tolerates minor `pg_restore` errors during schema restoration.
   > When `pg_restore` reports "errors ignored on restore: N" where N is at most
   > the `--restore-tolerance` value, pgcopydb logs a warning and continues.
   > This allows migrations to proceed through extension version mismatches that
   > do not affect data integrity.

4. Then as many as `--table-jobs` COPY sub-processes are started to share the
   workload and COPY the data from the source to the target database one table
   at a time, in a loop.

   A Postgres connection and a SQL query to the Postgres catalog table pg_class
   is used to get the list of tables with data to copy around, and the
   `reltuples` statistic is used to start with the tables with the greatest
   number of rows first, as an attempt to minimize the copy time.

5. An auxiliary process loops through all the Large Objects found on the source
   database and copies its data parts over to the target database, much like
   pg_dump itself would.

   This step is much like `pg_dump | pg_restore` for large objects data parts,
   except that there isn't a good way to do just that with the tooling.

6. As many as `--index-jobs` CREATE INDEX sub-processes are started to share the
   workload and build indexes. In order to make sure to start the CREATE INDEX
   commands only after the COPY operation has completed, a queue mechanism is
   used. As soon as a table data COPY has completed, all the indexes for the
   table are queued for processing by the CREATE INDEX sub-processes.

   The primary indexes are created as UNIQUE indexes at this stage.

7. Then the PRIMARY KEY constraints are created USING the just built indexes.
   This two-steps approach allows the primary key index itself to be created in
   parallel with other indexes on the same table, avoiding an EXCLUSIVE LOCK
   while creating the index.

8. As many as `--table-jobs` VACUUM ANALYZE sub-processes are started to share
   the workload. As soon as a table data COPY has completed, the table is queued
   for processing by the VACUUM ANALYZE sub-processes.

9. An auxiliary process loops over the sequences on the source database and for
   each of them runs a separate query on the source to fetch the `last_value`
   and the `is_called` metadata the same way that pg_dump does.

   For each sequence, pgcopydb then calls `pg_catalog.setval()` on the target
   database with the information obtained on the source database.

10. The final stage consists now of running the `pg_restore` command for the
    `post-data` section script for the whole database, and that's where the
    foreign key constraints and other elements are created.

    The *post-data* script is filtered out using the `pg_restore --use-list`
    option so that indexes and primary key constraints already created in steps
    6 and 7 are properly skipped now.

    This step uses as many as `--restore-jobs` jobs for `pg_restore` to share
    the workload and restore the objects in parallel.

### Postgres privileges, superuser, and dump and restore

Postgres has a notion of a superuser status that can be assigned to any role in
the system, and the default role *postgres* has this status. From the
[Role Attributes](https://www.postgresql.org/docs/current/role-attributes.html)
documentation page we see that:

> *A database superuser bypasses all permission checks, except the right to log
> in. This is a dangerous privilege and should not be used carelessly; it is best
> to do most of your work as a role that is not a superuser. To create a new
> database superuser, use CREATE ROLE name SUPERUSER. You must do this as a role
> that is already a superuser.*

Some Postgres objects can only be created by superusers, and some read and write
operations are only allowed to superuser roles, such as the following
non-exclusive list:

- Reading the
  [pg_authid](https://www.postgresql.org/docs/current/catalog-pg-authid.html)
  role password (even when encrypted) is restricted to roles with the superuser
  status. Reading this catalog table is done when calling
  `pg_dumpall --roles-only` so that the dump file can then be used to restore
  roles including their passwords.

  It is possible to implement a pgcopydb migration that skips the passwords
  entirely when using the option `--no-role-passwords`. In that case though
  authentication might fail until passwords have been setup again correctly.

- Most of the available Postgres extensions, at least when being written in C,
  are then only allowed to be created by roles with superuser status.

  When such an extension contains
  [Extension Configuration Tables](https://www.postgresql.org/docs/current/extend-extensions.html#EXTEND-EXTENSIONS-CONFIG-TABLES)
  and has been created with a role having superuser status, then the same
  superuser status is needed again to pg_dump and pg_restore that extension and
  its current configuration.

When using pgcopydb it is possible to split your migration in privileged and
non-privileged parts, like in the following examples:

```bash
$ coproc ( pgcopydb snapshot )

# first two commands would use a superuser role to connect
$ pgcopydb copy roles --source ... --target ...
$ pgcopydb copy extensions --source ... --target ...

# now it's possible to use a non-superuser role to connect
$ pgcopydb clone --skip-extensions --source ... --target ...

$ kill -TERM ${COPROC_PID}
$ wait ${COPROC_PID}
```

In such a script, the calls to
[pgcopydb copy roles](pgcopydb_copy.md#pgcopydb-copy-roles) and
[pgcopydb copy extensions](pgcopydb_copy.md#pgcopydb-copy-extensions) would be
done with connection strings that connects with a role having superuser status;
and then the call to *pgcopydb clone* would be done with a non-privileged role,
typically the role that owns the source and target databases.

> **Warning**
>
> That said, there is currently a limitation in `pg_dump` that impacts pgcopydb.
> When an extension with configuration table has been installed as superuser,
> even the main `pgcopydb clone` operation has to be done with superuser status.
>
> That's because pg_dump filtering (here, the `--exclude-table` option) does not
> apply to extension members, and pg_dump does not provide a mechanism to
> exclude extensions.

### Change Data Capture using Postgres Logical Decoding

When using the `--follow` option the steps from the
[pgcopydb follow](pgcopydb_follow.md) command are also run concurrently to the
main copy. The Change Data Capture is then automatically driven from a
prefetch-only phase to the prefetch-and-catchup phase, which is enabled as soon
as the base copy is done.

See the command
[pgcopydb stream sentinel set endpos](pgcopydb_stream.md#pgcopydb-stream-sentinel-set-endpos)
to remote control the follow parts of the command even while the command is
already running.

The command
[pgcopydb stream cleanup](pgcopydb_stream.md#pgcopydb-stream-cleanup) must be
used to free resources created to support the change data capture process.

> **Important**
>
> Make sure to read the documentation for
> [pgcopydb follow](pgcopydb_follow.md) and the specifics about
> [Logical Replication Restrictions](https://www.postgresql.org/docs/current/logical-replication-restrictions.html)
> as documented by Postgres.

#### Change Data Capture example 1

A simple approach to applying changes after the initial base copy has been done
follows:

```bash
$ pgcopydb clone --follow &

# later when the application is ready to make the switch
$ pgcopydb stream sentinel set endpos --current

# later when the migration is finished, clean-up both source and target
$ pgcopydb stream cleanup
```

#### Change Data Capture example 2

In some cases, it might be necessary to have more control over some of the steps
taken here. Given pgcopydb flexibility, it's possible to implement the following
steps:

1. Grab a snapshot from the source database and hold an open Postgres connection
   for the duration of the base copy.

   In case of crash or other problems with the main operations, it's then
   possible to resume processing of the base copy and the applying of the
   changes with the same snapshot again.

   This step is also implemented when using `pgcopydb clone --follow`. That
   said, if the command was interrupted (or crashed), then the snapshot would be
   lost.

2. Setup the logical decoding within the snapshot obtained in the previous step,
   and the replication tracking on the target database.

   The following SQL objects are then created:

   - a replication slot on the source database,
   - a replication origin on the target database.

   This step is also implemented when using `pgcopydb clone --follow`. There is
   no way to implement Change Data Capture with pgcopydb and skip creating those
   SQL objects.

3. Start the base copy of the source database, and prefetch logical decoding
   changes to ensure that we consume from the replication slot and allow the
   source database server to recycle its WAL files.

4. Remote control the apply process to stop consuming changes and applying them
   on the target database.

5. Re-sync the sequences to their now-current values.

   Sequences are not handled by Postgres logical decoding, so extra care needs
   to be implemented manually here.

6. Clean-up the specific resources created for supporting resumability of the
   whole process (replication slot on the source database, replication origin on
   the target database).

7. Stop holding a snapshot on the source database by stopping the
   `pgcopydb snapshot` process left running in the background.

If the command `pgcopydb clone --follow` fails it's then possible to start it
again. It will automatically discover what was done successfully and what needs
to be done again because it failed or was interrupted (table copy, index
creation, resuming replication slot consuming, resuming applying changes at the
right LSN position, etc).

Here is an example implementing the previous steps:

```bash
$ pgcopydb snapshot &

$ pgcopydb stream setup

$ pgcopydb clone --follow &

# later when the application is ready to make the switch
$ pgcopydb stream sentinel set endpos --current

# when the follow process has terminated, re-sync the sequences
$ pgcopydb copy sequences

# later when the migration is finished, clean-up both source and target
$ pgcopydb stream cleanup

# now stop holding the snapshot transaction (adjust PID to your environment)
$ kill %1
```

## Options

The following options are available to `pgcopydb clone`:

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

`--table-jobs`

How many tables can be processed in parallel.

This limit only applies to the COPY operations, more sub-processes will be
running at the same time that this limit while the CREATE INDEX operations are
in progress, though then the processes are only waiting for the target Postgres
instance to do all the work.

`--index-jobs`

How many indexes can be built in parallel, globally. A good option is to set
this option to the count of CPU cores that are available on the Postgres target
system, minus some cores that are going to be used for handling the COPY
operations.

`--restore-jobs`

How many threads or processes can be used during pg_restore. A good option is to
set this option to the count of CPU cores that are available on the Postgres
target system.

If this value is not set, we reuse the `--index-jobs` value. If that value is
not set either, we use the default value for `--index-jobs`.

`--large-object-jobs`

How many worker processes to start to copy Large Objects concurrently.

`--split-tables-larger-than`

Allow [same-table concurrency](../concurrency.md#same-table-concurrency) when
processing the source database. This value is expected to be a byte size, and
bytes units B, kB, MB, GB, TB, PB, and EB are known. See also
[Options for large migrations](../operations.md#same-table-concurrency).

`--split-max-parts`

Limit the maximum number of parts when same-table concurrency is used.

`--estimate-table-sizes`

Use estimates on table sizes to decide how to split tables when using same-table
concurrency.

When this option is used, we run `vacuumdb --analyze-only --jobs=<table-jobs>`
command on the source database that updates the statistics for the number of
pages for each relation. Later, we use the number of pages, and the size for
each page to estimate the actual size of the tables.

If you wish to run the ANALYZE command manually before running pgcopydb, you can
use the `--skip-analyze` option. This way, you can decrease the time spent on
the migration.

`--defer-indexes`

Defer index building until after all table data is copied, instead of building
the indexes of a table as soon as that table's COPY finishes. See
[Options for large migrations](../operations.md#index-builds).

`--defer-validate-fks`

Create foreign key constraints as `NOT VALID`, which skips the validation scan
of the rows that already exist. See
[Options for large migrations](../operations.md#foreign-keys).

`--defer-analyze`

Defer `VACUUM ANALYZE` until after the post-data restore.

`--restore-tolerance`

How many `pg_restore` errors to tolerate before the migration gives up. The
default is 10.

`--drop-if-exists`

When restoring the schema on the target Postgres instance, `pgcopydb` actually
uses `pg_restore`. When this option is specified, then the following pg_restore
options are also used: `--clean --if-exists`.

This option is useful when the same command is run several times in a row,
either to fix a previous mistake or for instance when used in a continuous
integration system.

This option causes `DROP TABLE` and `DROP INDEX` and other DROP commands to be
used. Make sure you understand what you're doing here!

`--roles`

The option `--roles` add a preliminary step that copies the roles found on the
source instance to the target instance. As Postgres roles are global object,
they do not exist only within the context of a specific database, so all the
roles are copied over when using this option.

The `pg_dumpall --roles-only` is used to fetch the list of roles from the source
database, and this command includes support for passwords. As a result, this
operation requires the superuser privileges.

See also [pgcopydb copy roles](pgcopydb_copy.md#pgcopydb-copy-roles).

`--no-role-passwords`

Do not dump passwords for roles. When restored, roles will have a null password,
and password authentication will always fail until the password is set. Since
password values aren't needed when this option is specified, the role
information is read from the catalog view pg_roles instead of pg_authid.
Therefore, this option also helps if access to pg_authid is restricted by some
security policy.

`--no-owner`

Do not output commands to set ownership of objects to match the original
database. By default, `pg_restore` issues `ALTER OWNER` or
`SET SESSION AUTHORIZATION` statements to set ownership of created schema
elements. These statements will fail unless the initial connection to the
database is made by a superuser (or the same user that owns all of the objects
in the script). With `--no-owner`, any user name can be used for the initial
connection, and this user will own all the created objects.

`--skip-large-objects`

Skip copying large objects, also known as blobs, when copying the data from the
source database to the target database.

`--skip-extensions`

Skip copying extensions from the source database to the target database.

When used, schema that extensions depend-on are also skipped: it is expected
that creating needed extensions on the target system is then the responsibility
of another command (such as
[pgcopydb copy extensions](pgcopydb_copy.md#pgcopydb-copy-extensions)), and
schemas that extensions depend-on are part of that responsibility.

Because creating extensions require superuser, this allows a multi-steps
approach where extensions are dealt with superuser privileges, and then the rest
of the pgcopydb operations are done without superuser privileges.

`--skip-ext-comments`

Skip copying COMMENT ON EXTENSION commands. This is implicit when using
`--skip-extensions`.

`--requirements <filename>`

This option allows to specify which version of an extension to install on the
target database. The given filename is expected to be a JSON file, and the JSON
contents must be an array of objects with the keys `"name"` and `"version"`.

The command `pgcopydb list extensions --requirements --json` produces such a
JSON file and can be used on the target database instance to get started.

See also the command `pgcopydb list extensions --available-versions`, and
[pgcopydb list extensions](pgcopydb_list.md#pgcopydb-list-extensions).

`--skip-collations`

Skip copying collations from the source database to the target database.

In some scenarios the list of collations provided by the Operating System on the
source and target system might be different, and a mapping then needs to be
manually installed before calling pgcopydb.

Then this option allows pgcopydb to skip over collations and assume all the
needed collations have been deployed on the target database already.

See also [pgcopydb list collations](pgcopydb_list.md#pgcopydb-list-collations).

`--skip-vacuum`

Skip running VACUUM ANALYZE on the target database once a table has been copied,
its indexes have been created, and constraints installed.

`--skip-analyze`

Skip running `vacuumdb --analyze-only` on the source database to update
statistics that are required when estimating table sizes.

This option is useful only when using `--estimate-table-sizes` and the user runs
the relevant ANALYZE command manually before running pgcopydb.

`--skip-db-properties`

Skip fetching database properties and copying them using the SQL command
`ALTER DATABASE ... SET name = value`. This is useful when the source and target
database have a different set of properties, or when the target database is
hosted in a way that disabled setting some of the properties that have been set
on the source database, or also when copying these settings is not wanted.

`--skip-split-by-ctid`

Skip splitting tables based on CTID during the copy operation. By default,
pgcopydb splits large tables into smaller chunks based on the CTID column if
there isn't a unique integer column in the table. However, in some cases you may
want to skip this splitting process if the CTID range scan is slow in the
underlying system.

`--filters <filename>`

This option allows to exclude table and indexes from the copy operations. See
[Filtering](pgcopydb_config.md#filtering) for details about the expected file
format and the filtering options available.

`--fail-fast`

Abort early in case of error by sending the TERM signal to all the processes in
the pgcopydb process group.

`--restart`

When running the pgcopydb command again, if the work directory already contains
information from a previous run, then the command refuses to proceed and delete
information that might be used for diagnostics and forensics.

In that case, the `--restart` option can be used to allow pgcopydb to delete
traces from a previous run.

`--resume`

When the pgcopydb command was terminated before completion, either by an
interrupt signal (such as C-c or SIGTERM) or because it crashed, it is possible
to resume the database migration.

When resuming activity from a previous run, table data that was fully copied
over to the target server is not sent again. Table data that was interrupted
during the COPY has to be started from scratch even when using `--resume`: the
COPY command in Postgres is transactional and was rolled back.

Same reasoning applies to the CREATE INDEX commands and ALTER TABLE commands
that pgcopydb issues, those commands are skipped on a `--resume` run only if
known to have run through to completion on the previous one.

Finally, using `--resume` requires the use of `--not-consistent`.

`--not-consistent`

In order to be consistent, pgcopydb exports a Postgres snapshot by calling the
[pg_export_snapshot()](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION-TABLE)
function on the source database server. The snapshot is then re-used in all the
connections to the source database server by using the
`SET TRANSACTION SNAPSHOT` command.

Per the Postgres documentation about `pg_export_snapshot`:

> Saves the transaction's current snapshot and returns a text string identifying
> the snapshot. This string must be passed (outside the database) to clients that
> want to import the snapshot. The snapshot is available for import only until
> the end of the transaction that exported it.

Now, when the pgcopydb process was interrupted (or crashed) on a previous run,
it is possible to resume operations, but the snapshot that was exported does not
exist anymore. The pgcopydb command can only resume operations with a new
snapshot, and thus can not ensure consistency of the whole data set, because
each run is now using their own snapshot.

`--snapshot`

Instead of exporting its own snapshot by calling the PostgreSQL function
`pg_export_snapshot()` it is possible for pgcopydb to re-use an already exported
snapshot.

`--follow`

When the `--follow` option is used then pgcopydb implements Change Data Capture
as detailed in the manual page for [pgcopydb follow](pgcopydb_follow.md) in
parallel to the main copy database steps.

The replication slot is created using the same snapshot as the main database
copy operation, and the changes to the source database are prefetched only
during the initial copy, then prefetched and applied in a catchup process.

It is possible to give `pgcopydb clone --follow` a termination point (the LSN
endpos) while the command is running with the command
[pgcopydb stream sentinel set endpos](pgcopydb_stream.md#pgcopydb-stream-sentinel-set-endpos).

`--plugin`

Logical decoding output plugin to use. The default is
[pgoutput](https://www.postgresql.org/docs/current/protocol-logical-replication.html),
which is built into Postgres core since version 10 and needs no extension on the
source server. See [pgcopydb follow](pgcopydb_follow.md) for the `--publication`
option that goes with it.

[test_decoding](https://www.postgresql.org/docs/current/test-decoding.html) also
ships with Postgres core and remains supported.

[wal2json](https://github.com/eulerto/wal2json/) remains supported, but since
CVE-2026-6471 you must add it to the `output_plugin_libraries` parameter on the
source server, which defaults to `pgoutput, test_decoding`.

`--publication`

Name of the publication to use with the `--plugin pgoutput` option. See
[pgcopydb follow](pgcopydb_follow.md) for the details.

`--wal2json-numeric-as-string`

When using the wal2json output plugin, it is possible to use the
`--wal2json-numeric-as-string` option to instruct wal2json to output numeric
values as strings and thus prevent some precision loss.

You need to have a wal2json plugin version on source database that supports
`--numeric-data-types-as-string` option to use this option.

See also the documentation for
[wal2json](https://github.com/eulerto/wal2json/pull/255) regarding this option
for details.

`--slot-name`

Logical decoding slot name to use. Defaults to `pgcopydb`, which is unfortunate
when your use-case involves migrating more than one database from the source
server.

`--create-slot`

Instruct pgcopydb to create the logical replication slot to use.

`--endpos`

Logical replication target LSN to use. Automatically stop replication and exit
with normal exit status 0 when receiving reaches the specified LSN. If there's a
record with LSN exactly equal to lsn, the record will be output.

The `--endpos` option is not aware of transaction boundaries and may truncate
output partway through a transaction. Any partially output transaction will not
be consumed and will be replayed again when the slot is next read from.
Individual messages are never truncated.

See also documentation for
[pg_recvlogical](https://www.postgresql.org/docs/current/app-pgrecvlogical.html).

`--prune-threshold`

Maximum total size of applied CDC files to retain, for example `10GB`. Set to
`0` to disable, which is the default. See
[Options for large migrations](../operations.md#disk-use-during-cdc).

`--prune-min-age`

Minimum age before an applied CDC file can be deleted, for example `15m`.

`--use-copy-binary`

Use the COPY WITH (FORMAT BINARY) instead of the COPY command.

See also documentation for
[COPY](https://www.postgresql.org/docs/current/sql-copy.html).

`--origin`

Logical replication target system needs to track the transactions that have been
applied already, so that in case we get disconnected or need to resume
operations we can skip already replayed transaction.

Postgres uses a notion of an origin node name as documented in
[Replication Progress Tracking](https://www.postgresql.org/docs/current/replication-origins.html).
This option allows to pick your own node name and defaults to "pgcopydb".
Picking a different name is useful in some advanced scenarios like migrating
several sources in the same target, where each source should have their own
unique origin node name.

`--verbose`, `--notice`

Increase current verbosity. The default level of verbosity is INFO. In ascending
order pgcopydb knows about the following verbosity levels: FATAL, ERROR, WARN,
INFO, NOTICE, SQL, DEBUG, TRACE.

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

`PGCOPYDB_TABLE_JOBS`

Number of concurrent jobs allowed to run COPY operations in parallel. When
`--table-jobs` is omitted from the command line, then this environment variable
is used.

`PGCOPYDB_INDEX_JOBS`

Number of concurrent jobs allowed to run CREATE INDEX operations in parallel.
When `--index-jobs` is omitted from the command line, then this environment
variable is used.

`PGCOPYDB_RESTORE_JOBS`

Number of concurrent jobs allowed to run `pg_restore` operations in parallel.
When `--restore-jobs` is omitted from the command line, then this environment
variable is used.

`PGCOPYDB_LARGE_OBJECTS_JOBS`

Number of concurrent jobs allowed to copy Large Objects data in parallel. When
`--large-objects-jobs` is omitted from the command line, then this environment
variable is used.

`PGCOPYDB_SPLIT_TABLES_LARGER_THAN`

Allow same-table concurrency when processing the source database. This
environment variable value is expected to be a byte size, and bytes units B, kB,
MB, GB, TB, PB, and EB are known.

When `--split-tables-larger-than` is omitted from the command line, then this
environment variable is used.

`PGCOPYDB_SPLIT_MAX_PARTS`

Limit the maximum number of parts when same-table concurrency is used. When
`--split-max-parts` is omitted from the command line, then this environment
variable is used.

`PGCOPYDB_ESTIMATE_TABLE_SIZES`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb estimates the size of tables to determine whether or not to split
tables. This option is only useful when querying the relation sizes on source
database is costly.

When `--estimate-table-sizes` is omitted from the command line, then this
environment variable is used.

When this option is used, we run `vacuumdb --analyze-only --jobs=<table-jobs>`
command on the source database that updates the statistics for the number of
pages for each relation. Later, we use the number of pages, and the size for
each page to estimate the actual size of the tables.

If you wish to run the ANALYZE command manually before running pgcopydb, you can
use the `--skip-analyze` option or `PGCOPYDB_SKIP_ANALYZE` environment variable.
This way, you can decrease the time spent on the migration.

`PGCOPYDB_OUTPUT_PLUGIN`

Logical decoding output plugin to use. When `--plugin` is omitted from the
command line, then this environment variable is used.

`PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb uses the wal2json option `--numeric-data-types-as-string` when using
the wal2json output plugin.

When `--wal2json-numeric-as-string` is omitted from the command line then this
environment variable is used.

`PGCOPYDB_DROP_IF_EXISTS`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb uses the pg_restore options `--clean --if-exists` when creating the
schema on the target Postgres instance.

When `--drop-if-exists` is omitted from the command line then this environment
variable is used.

`PGCOPYDB_FAIL_FAST`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb sends the TERM signal to all the processes in its process group as soon
as one process terminates with a non-zero return code.

When `--fail-fast` is omitted from the command line then this environment
variable is used.

`PGCOPYDB_SKIP_VACUUM`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb skips the VACUUM ANALYZE jobs entirely, same as when using the
`--skip-vacuum` option.

`PGCOPYDB_SKIP_ANALYZE`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb skips the `vacuumdb --analyze-only` commands entirely, same as when
using the `--skip-analyze` option.

`PGCOPYDB_SKIP_DB_PROPERTIES`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb skips the ALTER DATABASE SET properties commands that copy the setting
from the source to the target database, same as when using the
`--skip-db-properties` option.

`PGCOPYDB_SKIP_CTID_SPLIT`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb skips the CTID split operation during the clone process, same as when
using the `--skip-split-by-ctid` option.

`PGCOPYDB_USE_COPY_BINARY`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb uses the COPY WITH (FORMAT BINARY) instead of the COPY command, same as
when using the `--use-copy-binary` option.

`PGCOPYDB_SNAPSHOT`

Postgres snapshot identifier to re-use, see also `--snapshot`.

`TMPDIR`

The pgcopydb command creates all its work files and directories in
`${TMPDIR}/pgcopydb`, and defaults to `/tmp/pgcopydb`.

`PGCOPYDB_LOG_TIME_FORMAT`

The logs time format defaults to `%H:%M:%S` when pgcopydb is used on an
interactive terminal, and to `%Y-%m-%d %H:%M:%S` otherwise. This environment
variable can be set to any format string other than the defaults.

See documentation for strftime(3) for details about the format string. See
documentation for isatty(3) for details about detecting if pgcopydb is run in an
interactive terminal.

`PGCOPYDB_LOG_JSON`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb formats its logs using JSON.

```json
{
  "timestamp": "2026-04-13 16:53:14",
  "pid": 87956,
  "error_level": 4,
  "error_severity": "INFO",
  "file_name": "main.c",
  "file_line_num": 165,
  "message": "Running pgcopydb version 0.19.0"
}
```

`PGCOPYDB_LOG_FILENAME`

When set to a filename (in a directory that must exists already) then pgcopydb
writes its logs output to that filename in addition to the logs on the standard
error output stream.

If the file already exists, its content is overwritten. In other words the
previous content would be lost when running the same command twice.

`PGCOPYDB_LOG_JSON_FILE`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb formats its logs using JSON when writing to PGCOPYDB_LOG_FILENAME.

`XDG_DATA_HOME`

The standard
[XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
defines several environment variables that allow controlling where programs
should store their files.

> *XDG_DATA_HOME defines the base directory relative to which user-specific data
> files should be stored. If $XDG_DATA_HOME is either not set or empty, a
> default equal to $HOME/.local/share should be used.*

When using Change Data Capture (through `--follow` option and Postgres logical
decoding) then pgcopydb pre-fetches changes in JSON files and transform them
into SQL files to apply to the target database.

These files are stored at the following location, tried in this order:

1. when `--dir` is used, then pgcopydb uses the `cdc` subdirectory of the
   `--dir` location,

2. when `XDG_DATA_HOME` is set in the environment, then pgcopydb uses that
   location,

3. when neither of the previous settings have been used then pgcopydb defaults
   to using `${HOME}/.local/share`.

## Examples

```
$ export PGCOPYDB_SOURCE_PGURI=postgres://pagila:0wn3d@source/pagila
$ export PGCOPYDB_TARGET_PGURI=postgres://pagila:0wn3d@target/pagila
$ export PGCOPYDB_DROP_IF_EXISTS=on

$ pgcopydb clone --table-jobs 8 --index-jobs 12
08:13:13.961 42893 INFO   [SOURCE] Copying database from "postgres://pagila:0wn3d@source/pagila"
08:13:13.961 42893 INFO   [TARGET] Copying database into "postgres://pagila:0wn3d@target/pagila"
08:13:14.009 42893 INFO   Using work dir "/tmp/pgcopydb"
08:13:14.017 42893 INFO   Exported snapshot "00000003-000000EB-1" from the source database
08:13:14.019 42904 INFO   STEP 1: fetch source database tables, indexes, and sequences
08:13:14.339 42904 INFO   Fetched information for 5 tables (including 0 tables split in 0 partitions total), with an estimated total of 1000 thousands tuples and 128 MB on-disk
08:13:14.342 42904 INFO   Fetched information for 4 indexes (supporting 4 constraints)
08:13:14.343 42904 INFO   Fetching information for 1 sequences
08:13:14.353 42904 INFO   Fetched information for 1 extensions
08:13:14.436 42904 INFO   Found 1 indexes (supporting 1 constraints) in the target database
08:13:14.443 42904 INFO   STEP 2: dump the source database schema (pre/post data)
08:13:14.448 42904 INFO    /usr/bin/pg_dump -Fc --snapshot 00000003-000000EB-1 --section=pre-data --section=post-data --file /tmp/pgcopydb/schema/schema.dump 'postgres://pagila:0wn3d@source/pagila'
08:13:14.513 42904 INFO   STEP 3: restore the pre-data section to the target database
08:13:14.524 42904 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila:0wn3d@target/pagila' --section pre-data --jobs 2 --use-list /tmp/pgcopydb/schema/pre-filtered.list /tmp/pgcopydb/schema/schema.dump
08:13:14.608 42919 INFO   STEP 4: starting 8 table-data COPY processes
08:13:14.678 42921 INFO   STEP 8: starting 8 VACUUM processes
08:13:14.678 42904 INFO   Skipping large objects: none found.
08:13:14.693 42920 INFO   STEP 6: starting 2 CREATE INDEX processes
08:13:14.693 42920 INFO   STEP 7: constraints are built by the CREATE INDEX processes
08:13:14.699 42904 INFO   STEP 9: reset sequences values
08:13:14.700 42959 INFO   Set sequences values on the target database
08:13:16.716 42904 INFO   STEP 10: restore the post-data section to the target database
08:13:16.726 42904 INFO    /usr/bin/pg_restore --dbname 'postgres://pagila:0wn3d@target/pagila' --section post-data --jobs 2 --use-list /tmp/pgcopydb/schema/post-filtered.list /tmp/pgcopydb/schema/schema.dump
08:13:16.751 42904 INFO   All step are now done,  2s728 elapsed
08:13:16.752 42904 INFO   Printing summary for 5 tables and 4 indexes

  OID | Schema |             Name | Parts | copy duration | transmitted bytes | indexes | create index duration
------+--------+------------------+-------+---------------+-------------------+---------+----------------------
16398 | public | pgbench_accounts |     1 |         1s496 |             91 MB |       1 |                 302ms
16395 | public |  pgbench_tellers |     1 |          37ms |            1002 B |       1 |                  15ms
16401 | public | pgbench_branches |     1 |          45ms |              71 B |       1 |                  18ms
16386 | public |           table1 |     1 |          36ms |             984 B |       1 |                  21ms
16392 | public |  pgbench_history |     1 |          41ms |               0 B |       0 |                   0ms


                                               Step   Connection    Duration    Transfer   Concurrency
 --------------------------------------------------   ----------  ----------  ----------  ------------
   Catalog Queries (table ordering, filtering, etc)       source       119ms                         1
                                        Dump Schema       source        66ms                         1
                                     Prepare Schema       target        59ms                         1
      COPY, INDEX, CONSTRAINTS, VACUUM (wall clock)         both       2s125                        18
                                  COPY (cumulative)         both       1s655      128 MB             8
                          CREATE INDEX (cumulative)       target       343ms                         2
                           CONSTRAINTS (cumulative)       target        13ms                         2
                                VACUUM (cumulative)       target       144ms                         8
                                    Reset Sequences         both        15ms                         1
                         Large Objects (cumulative)       (null)         0ms                         0
                                    Finalize Schema         both        27ms                         2
 --------------------------------------------------   ----------  ----------  ----------  ------------
```

# pgcopydb restore

pgcopydb restore - Restore database objects into a Postgres instance

This command prefixes the following sub-commands:

<!-- BEGIN HELP: pgcopydb restore -->
```
pgcopydb restore: Restore database objects into a Postgres instance

Available commands:
  pgcopydb restore
    schema      Restore a database schema from custom files to target database
    pre-data    Restore a database pre-data schema from custom file to target database
    post-data   Restore a database post-data schema from custom file to target database
    roles       Restore database roles from SQL file to target database
    parse-list  Parse pg_restore --list output from custom file
```
<!-- END HELP -->

## pgcopydb restore schema

pgcopydb restore schema - Restore a database schema from custom files to target
database

The command `pgcopydb restore schema` uses pg_restore to create the SQL schema
definitions from the given `pgcopydb dump schema` export directory. This command
is not compatible with using Postgres files directly, it must be fed with the
directory output from the `pgcopydb dump ...` commands.

<!-- BEGIN HELP: pgcopydb restore schema -->
```
pgcopydb restore schema: Restore a database schema from custom files to target database
usage: pgcopydb restore schema  --dir <dir> [ --source <URI> ] --target <URI> 

  --source             Postgres URI to the source database
  --target             Postgres URI to the target database
  --dir                Work directory to use
  --restore-jobs       Number of concurrent jobs for pg_restore
  --restore-tolerance  Max pg_restore errors to tolerate (default 10)
  --drop-if-exists     On the target database, clean-up from a previous run first
  --no-owner           Do not set ownership of objects to match the original database
  --no-acl             Prevent restoration of access privileges (grant/revoke commands).
  --no-comments        Do not output commands to restore comments
  --no-tablespaces     Do not output commands to select tablespaces
  --filters <filename> Use the filters defined in <filename>
  --restart            Allow restarting when temp files exist already
  --resume             Allow resuming operations after a failure
  --not-consistent     Allow taking a new snapshot on the source database
```
<!-- END HELP -->

## pgcopydb restore pre-data

pgcopydb restore pre-data - Restore a database pre-data schema from custom file
to target database

The command `pgcopydb restore pre-data` uses pg_restore to create the SQL schema
definitions from the given `pgcopydb dump schema` export directory. This command
is not compatible with using Postgres files directly, it must be fed with the
directory output from the `pgcopydb dump ...` commands.

<!-- BEGIN HELP: pgcopydb restore pre-data -->
```
pgcopydb restore pre-data: Restore a database pre-data schema from custom file to target database
usage: pgcopydb restore pre-data  --dir <dir> [ --source <URI> ] --target <URI> 

  --source             Postgres URI to the source database
  --target             Postgres URI to the target database
  --dir                Work directory to use
  --restore-jobs       Number of concurrent jobs for pg_restore
  --restore-tolerance  Max pg_restore errors to tolerate (default 10)
  --drop-if-exists     On the target database, clean-up from a previous run first
  --no-owner           Do not set ownership of objects to match the original database
  --no-acl             Prevent restoration of access privileges (grant/revoke commands).
  --no-comments        Do not output commands to restore comments
  --no-tablespaces     Do not output commands to select tablespaces
  --skip-extensions    Skip restoring extensions
  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION
  --skip-collations    Skip restoring collations
  --skip-publications  Skip restoring publications
  --filters <filename> Use the filters defined in <filename>
  --restart            Allow restarting when temp files exist already
  --resume             Allow resuming operations after a failure
  --not-consistent     Allow taking a new snapshot on the source database
```
<!-- END HELP -->

## pgcopydb restore post-data

pgcopydb restore post-data - Restore a database post-data schema from custom
file to target database

The command `pgcopydb restore post-data` uses pg_restore to create the SQL
schema definitions from the given `pgcopydb dump schema` export directory. This
command is not compatible with using Postgres files directly, it must be fed
with the directory output from the `pgcopydb dump ...` commands.

<!-- BEGIN HELP: pgcopydb restore post-data -->
```
pgcopydb restore post-data: Restore a database post-data schema from custom file to target database
usage: pgcopydb restore post-data  --dir <dir> [ --source <URI> ] --target <URI> 

  --source             Postgres URI to the source database
  --target             Postgres URI to the target database
  --dir                Work directory to use
  --restore-jobs       Number of concurrent jobs for pg_restore
  --restore-tolerance  Max pg_restore errors to tolerate (default 10)
  --no-owner           Do not set ownership of objects to match the original database
  --no-acl             Prevent restoration of access privileges (grant/revoke commands).
  --no-comments        Do not output commands to restore comments
  --no-tablespaces     Do not output commands to select tablespaces
  --skip-extensions    Skip restoring extensions
  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION
  --skip-collations    Skip restoring collations
  --skip-publications  Skip restoring publications
  --filters <filename> Use the filters defined in <filename>
  --restart            Allow restarting when temp files exist already
  --resume             Allow resuming operations after a failure
  --not-consistent     Allow taking a new snapshot on the source database
```
<!-- END HELP -->

## pgcopydb restore roles

pgcopydb restore roles - Restore database roles from SQL file to target database

The command `pgcopydb restore roles` runs the commands from the SQL script
obtained from the command `pgcopydb dump roles`. Roles that already exist on the
target database are skipped.

The `pg_dumpall` command issues two lines per role, the first one is a
`CREATE ROLE` SQL command, the second one is an `ALTER ROLE` SQL command. Both
those lines are skipped when the role already exists on the target database.

<!-- BEGIN HELP: pgcopydb restore roles -->
```
pgcopydb restore roles: Restore database roles from SQL file to target database
usage: pgcopydb restore roles  --dir <dir> [ --source <URI> ] --target <URI> 

  --source             Postgres URI to the source database
  --target             Postgres URI to the target database
  --dir                Work directory to use
  --restore-jobs       Number of concurrent jobs for pg_restore
  --restore-tolerance  Max pg_restore errors to tolerate (default 10)
```
<!-- END HELP -->

## pgcopydb restore parse-list

pgcopydb restore parse-list - Parse pg_restore --list output from custom file

The command `pgcopydb restore parse-list` outputs pg_restore to list the archive
catalog of the custom file format file that has been exported for the post-data
section.

When using the `--filters` option, then the source database connection is used
to grab all the dependent objects that should also be filtered, and the output
of the command shows those pg_restore catalog entries commented out.

A pg_restore archive catalog entry is commented out when its line starts with a
semi-colon character (`;`).

<!-- BEGIN HELP: pgcopydb restore parse-list -->
```
pgcopydb restore parse-list: Parse pg_restore --list output from custom file
usage: pgcopydb restore parse-list  [ <pre.list> ] 

  --source             Postgres URI to the source database
  --target             Postgres URI to the target database
  --dir                Work directory to use
  --filters <filename> Use the filters defined in <filename>
  --skip-extensions    Skip restoring extensions
  --skip-ext-comments  Skip restoring COMMENT ON EXTENSION
  --restart            Allow restarting when temp files exist already
  --resume             Allow resuming operations after a failure
  --not-consistent     Allow taking a new snapshot on the source database
```
<!-- END HELP -->

## Description

The `pgcopydb restore schema` command implements the creation of SQL objects in
the target database, second and last steps of a full database migration.

When the command runs, it calls `pg_restore` on the files found at the expected
location within the `--target` directory, which has typically been created with
the `pgcopydb dump schema` command.

The `pgcopydb restore pre-data` and `pgcopydb restore post-data` are limiting
their actions to the file with pre-data and post-data in the source directory.

## Options

The following options are available to `pgcopydb restore schema`:

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

`--restore-jobs`

How many threads or processes can be used during pg_restore. A good option is to
set this option to the count of CPU cores that are available on the Postgres
target system.

If this value is not set, we reuse the `--index-jobs` value. If that value is not
set either, we use the default value for `--index-jobs`.

`--drop-if-exists`

When restoring the schema on the target Postgres instance, `pgcopydb` actually
uses `pg_restore`. When this option is specified, then the following pg_restore
options are also used: `--clean --if-exists`.

This option is useful when the same command is run several times in a row,
either to fix a previous mistake or for instance when used in a continuous
integration system.

This option causes `DROP TABLE` and `DROP INDEX` and other DROP commands to be
used. Make sure you understand what you're doing here!

`--no-owner`

Do not output commands to set ownership of objects to match the original
database. By default, `pg_restore` issues `ALTER OWNER` or
`SET SESSION AUTHORIZATION` statements to set ownership of created schema
elements. These statements will fail unless the initial connection to the
database is made by a superuser (or the same user that owns all of the objects
in the script). With `--no-owner`, any user name can be used for the initial
connection, and this user will own all the created objects.

`--filters <filename>`

This option allows to exclude table and indexes from the copy operations. See
[Filtering](pgcopydb_config.md#filtering) for details about the expected file
format and the filtering options available.

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

`PGCOPYDB_TARGET_PGURI`

Connection string to the target Postgres instance. When `--target` is omitted
from the command line, then this environment variable is used.

`PGCOPYDB_DROP_IF_EXISTS`

When true (or *yes*, or *on*, or 1, same input as a Postgres boolean) then
pgcopydb uses the pg_restore options `--clean --if-exists` when creating the
schema on the target Postgres instance.

## Examples

First, using `pgcopydb restore schema`:

```
$ PGCOPYDB_DROP_IF_EXISTS=on pgcopydb restore schema --source /tmp/target/ --target "port=54314 dbname=demo"
07:45:10.626 39254 INFO   Using work dir "/tmp/pgcopydb"
07:45:10.626 39254 INFO   Restoring database from existing files at "/tmp/pgcopydb"
07:45:10.720 39254 INFO   Found 2 indexes (supporting 2 constraints) in the target database
07:45:10.723 39254 INFO   Using pg_restore for Postgres "18.0" at "/usr/bin/pg_restore"
07:45:10.723 39254 INFO   [TARGET] Restoring database into "postgres://postgres@127.0.0.1:5435/demo?keepalives=1&keepalives_idle=10&keepalives_interval=10&keepalives_count=60"
07:45:10.737 39254 INFO   Drop tables on the target database, per --drop-if-exists
07:45:10.750 39254 INFO    /usr/bin/pg_restore --dbname 'postgres://postgres@127.0.0.1:5435/demo' --section pre-data --jobs 4 --clean --if-exists --use-list /tmp/pgcopydb/schema/pre-filtered.list /tmp/pgcopydb/schema/schema.dump
07:45:10.803 39254 INFO    /usr/bin/pg_restore --dbname 'postgres://postgres@127.0.0.1:5435/demo' --section post-data --jobs 4 --clean --if-exists --use-list /tmp/pgcopydb/schema/post-filtered.list /tmp/pgcopydb/schema/schema.dump
```

Then the `pgcopydb restore pre-data` and `pgcopydb restore post-data` would look
the same with just a single call to pg_restore instead of the both of them.

Using `pgcopydb restore parse-list` it's possible to review the filtering
options and see how pg_restore catalog entries are being commented-out.

```
$ cat ./tests/filtering/include.ini
[include-only-table]
public.actor
public.category
public.film
public.film_actor
public.film_category
public.language
public.rental

[exclude-index]
public.idx_store_id_film_id

[exclude-table-data]
public.rental

$ pgcopydb restore parse-list --dir /tmp/pagila/pgcopydb --resume --not-consistent --filters ./tests/filtering/include.ini
11:41:22 75175 INFO  Running pgcopydb version 0.19.0
11:41:22 75175 INFO  [SOURCE] Restoring database from "postgres://@:54311/pagila?"
11:41:22 75175 INFO  [TARGET] Restoring database into "postgres://@:54311/plop?"
11:41:22 75175 INFO  Using work dir "/tmp/pagila/pgcopydb"
11:41:22 75175 INFO  Schema dump for pre-data and post-data section have been done
11:41:22 75175 INFO  Restoring database from existing files at "/tmp/pagila/pgcopydb"
11:41:22 75175 INFO  Using pg_restore for Postgres "18.0" at "/usr/bin/pg_restore"
11:41:22 75175 INFO  Exported snapshot "00000003-0003209A-1" from the source database
3242; 2606 317973 CONSTRAINT public actor actor_pkey postgres
;3258; 2606 317975 CONSTRAINT public address address_pkey postgres
3245; 2606 317977 CONSTRAINT public category category_pkey postgres
;3261; 2606 317979 CONSTRAINT public city city_pkey postgres
;3264; 2606 317981 CONSTRAINT public country country_pkey postgres
;3237; 2606 317983 CONSTRAINT public customer customer_pkey postgres
3253; 2606 317985 CONSTRAINT public film_actor film_actor_pkey postgres
3256; 2606 317987 CONSTRAINT public film_category film_category_pkey postgres
3248; 2606 317989 CONSTRAINT public film film_pkey postgres
;3267; 2606 317991 CONSTRAINT public inventory inventory_pkey postgres
3269; 2606 317993 CONSTRAINT public language language_pkey postgres
3293; 2606 317995 CONSTRAINT public rental rental_pkey postgres
3246; 1259 318000 INDEX public film_fulltext_idx postgres
3243; 1259 318001 INDEX public idx_actor_last_name postgres
;3238; 1259 318002 INDEX public idx_fk_address_id postgres
3254; 1259 318006 INDEX public idx_fk_film_id postgres
3290; 1259 318007 INDEX public idx_fk_inventory_id postgres
;3265; 1259 318025 INDEX public idx_store_id_film_id postgres
3251; 1259 318026 INDEX public idx_title postgres
3350; 2620 318035 TRIGGER public film film_fulltext_trigger postgres
3348; 2620 318036 TRIGGER public actor last_updated postgres
;3354; 2620 318037 TRIGGER public address last_updated postgres
3315; 2606 318070 FK CONSTRAINT public film_actor film_actor_actor_id_fkey postgres
3316; 2606 318075 FK CONSTRAINT public film_actor film_actor_film_id_fkey postgres
;3341; 2606 318200 FK CONSTRAINT public rental rental_customer_id_fkey postgres
```

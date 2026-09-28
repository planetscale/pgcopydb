Options for large migrations
============================

A migration of a few gigabytes needs no tuning. The options on this page exist
because of migrations that do not fit that shape: a terabyte of data, a table
whose TOAST is a hundred times its heap, a source that will not hold a snapshot
open for six hours, or a target that drops a connection in the middle of the
run.

Each option below names the problem it solves. Reach for one when you have that
problem, not before.

Index builds
------------

By default pgcopydb builds the indexes of a table as soon as that table's COPY
finishes, so index builds and remaining table copies run at the same time. On a
large migration those index builds compete with COPY for CPU, memory and write
bandwidth on the target.

``--defer-indexes`` holds every index build until all table data is copied. The
COPY phase then runs at full speed, and the index builds follow with the whole
machine available to them.

The cost is that the target has no indexes until the copy finishes. Nothing can
read the target usefully until the index phase completes, and the index phase on
a large database is long.

Foreign keys
------------

pgcopydb creates foreign key constraints itself rather than leaving them to
``pg_restore``. When a constraint fails because the data already violates it
(SQLSTATE 23503), pgcopydb retries it as ``NOT VALID``. The constraint stays in
the schema and Postgres enforces it on every new write, so the migration
continues instead of aborting on data that was already in the source. This is
always on and needs no option.

``--defer-validate-fks`` goes further and creates every foreign key as
``NOT VALID`` from the start. Adding a validated foreign key makes Postgres scan
the whole referencing table to prove the existing rows satisfy it, and on a
large table that scan can take longer than the copy did. Skipping it hands the
copy to CDC immediately.

The constraint still enforces referential integrity on every new write, which is
what makes this safe with ``--follow``. What you give up is the proof that the
rows copied from the source satisfy it. Validate later, on your own schedule::

  ALTER TABLE myschema.mytable VALIDATE CONSTRAINT myconstraint;

pgcopydb reminds you which constraints are unvalidated at the end of the run,
and again at the end of ``follow``.

Analyze
-------

``--defer-analyze`` holds ``VACUUM ANALYZE`` until after the post-data restore,
so it does not compete with index builds and constraint creation.

Same-table concurrency
----------------------

A single table large enough to dominate the migration is copied by one worker
by default, so the migration ends up waiting on that one table.

``--split-tables-larger-than`` sets the size at which pgcopydb splits a table
across several COPY jobs, and ``--split-max-parts`` caps how many.

pgcopydb splits on a single-column integer unique key when the table has one.
Otherwise it falls back to splitting the table's CTID page range. The part count
comes from the table's total on-disk size, heap plus TOAST, so a table with a
small heap and a very large TOAST is still split into enough parts to be worth
it. A CTID range scan detoasts rows as it reads them, so splitting the heap
range parallelizes the TOAST reads too.

``--estimate-table-sizes`` uses the planner's estimates instead of measuring
each relation, which is faster on a database with many tables. The estimate is
heap only, so a table whose size is mostly TOAST may be split into fewer parts
than its real size deserves.

Restore errors
--------------

``--restore-tolerance`` sets how many ``pg_restore`` errors the migration
accepts before it gives up. The default is 10. Raise it when you expect a known
set of schema objects to fail on the target and you intend to fix them
afterwards. Lower it to zero when you want the run to stop at the first
surprise.

Disk use during CDC
-------------------

``pgcopydb follow`` writes the changes it receives to ``.json`` files and the
SQL it derives from them to ``.sql`` files. On a long migration of a write-heavy
source those files accumulate until they fill the disk.

``--prune-threshold`` sets the total size of applied CDC files to keep, for
example ``10GB``. Once the applied files pass that size, pgcopydb deletes the
oldest first. ``--prune-min-age``, for example ``15m``, sets how long a file
must have existed before it can be deleted, which leaves a window for debugging.

Pruning only ever touches files that are fully applied, which means their WAL
segment is behind ``replay_lsn``. It is disabled by default; set a threshold to
turn it on.

Sources that move under you
---------------------------

Two things can change about a source during a long migration.

A connection can drop. pgcopydb retries a table COPY after a source connection
failure, retries the post-copy ``VACUUM ANALYZE`` when the target connection
drops, and reconnects the CDC pipeline with exponential backoff when the target
goes away mid-stream.

The source's history can fork. A failover promotes a standby, and a
point-in-time restore rewinds and replays. Either one moves the source to a new
timeline, and changes already applied to the target may belong to a branch of
history the new source does not have. This matters most when the source is a
replica, and from PostgreSQL 17 a logical slot can survive a promotion, so the
stream does not simply end.

pgcopydb records the source timeline and compares it on every reconnect and
every resume. When it changes, the migration stops and says so rather than
continuing against a history that no longer matches what it has applied.
Compare the source and the target before you start a new migration.

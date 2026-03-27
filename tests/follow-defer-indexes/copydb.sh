#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# ensure TMPDIR is writable by the docker user
sudo mkdir -p ${TMPDIR}
sudo chown -R `whoami` ${TMPDIR}

# make sure source and target databases are ready
pgcopydb ping

# Load pagila schema and data on the source
psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

#
# Count indexes on the source before clone starts (for verification later).
#
src_index_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*)
     FROM pg_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")

echo "=== Source has ${src_index_count} indexes ==="

if [ "${src_index_count}" -lt 1 ]; then
    echo "ERROR: Expected at least 1 index on the source, found ${src_index_count}"
    exit 1
fi

#
# Count FK constraints on the source for verification.
#
src_fk_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace")

echo "=== Source has ${src_fk_count} foreign key constraints ==="

#
# Run pgcopydb clone --follow --defer-indexes.
#
# With --defer-indexes and --follow:
#   - STEP 6 (index supervisor) is skipped during the COPY phase
#   - STEP 7 (constraints) is skipped during the COPY phase
#   - STEP 8 (vacuum) is skipped during the COPY phase
#   - Clone subprocess exits early after COPY + blobs
#   - Parent process runs STEP 10 which builds indexes via
#     copydb_copy_all_indexes() using CREATE INDEX (ShareLock) + ALTER
#     TABLE ADD CONSTRAINT USING INDEX, then pg_restore handles remaining
#     post-data items (triggers, etc.)
#
# The inject service will:
#   1. Wait for the clone to start
#   2. Verify the snapshot is released after COPY completes
#   3. Inject DML changes and set endpos
#
pgcopydb clone \
         --follow \
         --defer-indexes \
         --plugin wal2json \
         --notice

# cleanup
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2
pgcopydb stream cleanup

#
# Verify data matches between source and target.
#
sql="select count(*), sum(amount) from payment"

src_result=`psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c "${sql}"`
tgt_result=`psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "${sql}"`

echo "Source: ${src_result}"
echo "Target: ${tgt_result}"

if [ "${src_result}" != "${tgt_result}" ]; then
    echo "ERROR: Source and target payment data do not match!"
    echo "  Source: ${src_result}"
    echo "  Target: ${tgt_result}"
    exit 1
fi

#
# Verify that indexes exist on the target (built by STEP 10).
#
tgt_index_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')")

echo "Source index count: ${src_index_count}"
echo "Target index count: ${tgt_index_count}"

if [ "${tgt_index_count}" -lt "${src_index_count}" ]; then
    echo "ERROR: Target has fewer indexes (${tgt_index_count}) than source (${src_index_count})!"
    echo "  This means STEP 10 did not build all indexes."
    exit 1
fi

#
# Verify FK constraints exist on the target.
#
tgt_fk_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace = 'public'::regnamespace")

echo "Source FK count: ${src_fk_count}"
echo "Target FK count: ${tgt_fk_count}"

if [ "${tgt_fk_count}" -lt "${src_fk_count}" ]; then
    echo "ERROR: Target has fewer FK constraints (${tgt_fk_count}) than source (${src_fk_count})!"
    exit 1
fi

#
# Verify a known primary key exists.
#
actor_pk=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*)
     FROM pg_indexes
    WHERE indexname = 'actor_pkey'")

if [ "${actor_pk}" != "1" ]; then
    echo "ERROR: actor_pkey index not found on target!"
    exit 1
fi

echo ""
echo "follow-defer-indexes test: PASSED"
echo ""
echo "Clone --follow --defer-indexes completed successfully:"
echo "  - STEP 6/7/8 were deferred to STEP 10"
echo "  - STEP 10 built ${tgt_index_count} indexes via parallel CREATE INDEX"
echo "  - ${tgt_fk_count} FK constraints created"
echo "  - Data is consistent between source and target"
echo "  - CDC replay worked correctly with deferred indexes"

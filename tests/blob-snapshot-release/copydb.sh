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

# make sure source and target databases are ready
pgcopydb ping

# Load test data with a large object on the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/setup.sql

# Verify large objects exist (this is the code path we're testing)
lo_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM pg_largeobject_metadata")

echo "Source has ${lo_count} large objects"

if [ "${lo_count}" -lt 1 ]; then
    echo "ERROR: Expected at least 1 large object on the source"
    exit 1
fi

#
# Count idle-in-transaction connections BEFORE clone
#
idle_txn_before=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM pg_stat_activity
    WHERE state = 'idle in transaction'
      AND query LIKE '%pg_largeobject_metadata%'")

echo "idle-in-transaction blob connections before clone: ${idle_txn_before}"

#
# Run pgcopydb clone (without --follow, simpler for this test)
#
pgcopydb clone --notice

#
# After clone completes, verify no idle-in-transaction connections remain
# that reference the blob check query. Before the fix, the connection from
# copydb_has_large_objects() would stay open as "idle in transaction" with
# query "select exists(select 1 from pg_largeobject_metadata)" for the
# entire clone duration.
#
idle_txn_after=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM pg_stat_activity
    WHERE state = 'idle in transaction'
      AND query LIKE '%pg_largeobject_metadata%'")

echo "idle-in-transaction blob connections after clone: ${idle_txn_after}"

if [ "${idle_txn_after}" != "0" ]; then
    echo "ERROR: Found ${idle_txn_after} idle-in-transaction connections"
    echo "  with pg_largeobject_metadata query after clone completed."
    echo "  copydb_has_large_objects() is not closing its snapshot connection."

    psql -d ${PGCOPYDB_SOURCE_PGURI} -c \
      "SELECT pid, state, backend_xmin, now() - xact_start AS duration, query
         FROM pg_stat_activity
        WHERE state = 'idle in transaction'"

    exit 1
fi

#
# Verify data was copied correctly
#
src_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c "SELECT count(*) FROM test_data")
tgt_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM test_data")

echo "Source rows: ${src_count}, Target rows: ${tgt_count}"

if [ "${src_count}" != "${tgt_count}" ]; then
    echo "ERROR: Row count mismatch! Source=${src_count} Target=${tgt_count}"
    exit 1
fi

#
# Verify large objects were copied
#
tgt_lo_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM pg_largeobject_metadata")

echo "Target large objects: ${tgt_lo_count}"

if [ "${tgt_lo_count}" != "${lo_count}" ]; then
    echo "ERROR: Large object count mismatch! Source=${lo_count} Target=${tgt_lo_count}"
    exit 1
fi

echo ""
echo "blob-snapshot-release test: PASSED"
echo "  - copydb_has_large_objects() closed its snapshot connection"
echo "  - No idle-in-transaction connections left behind"
echo "  - Data and large objects copied correctly"

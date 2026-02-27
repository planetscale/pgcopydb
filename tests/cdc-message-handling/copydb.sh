#!/bin/bash

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

# Create test schema
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Send messages from various "tools" (these should be filtered by wal2json)
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'
SELECT pg_logical_emit_message(false, 'peerdb_heartbeat', '');
SELECT pg_logical_emit_message(false, 'debezium.heartbeat', '{"ts_ms": 123456}');
SELECT pg_logical_emit_message(false, 'custom_tool', 'metadata');
SQL

# Make actual data changes
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'
INSERT INTO test_table (id, name) VALUES (1, 'test1'), (2, 'test2');
SQL

# Send more messages
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'
SELECT pg_logical_emit_message(false, 'peerdb_heartbeat', '');
SQL

# create the replication slot that captures all the changes
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject more SQL DML changes and messages
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'
SELECT pg_logical_emit_message(false, 'peerdb_heartbeat', '');
INSERT INTO test_table (id, name) VALUES (3, 'test3');
SELECT pg_logical_emit_message(false, 'debezium.heartbeat', '{}');
SQL

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# and prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn}"

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the SQL to the target database
pgcopydb stream catchup --resume --endpos "${lsn}"

# Verify data was copied
count=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM test_table;")
if [ "$count" -eq "3" ]; then
    echo "✓ Data was copied successfully (3 rows)"
else
    echo "✗ Data copy failed (expected 3, got $count)"
    exit 1
fi

# Check that JSON files do NOT contain action:"M" (filtered at source)
SHAREDIR=/var/lib/postgres/.local/share/pgcopydb
if grep -r '"action":"M"' ${SHAREDIR}/stream/ 2>/dev/null; then
    echo "✗ FAILED: Found MESSAGE actions in JSON files (wal2json filter didn't work)"
    exit 1
else
    echo "✓ No MESSAGE actions in JSON files (wal2json filter working)"
fi

echo "✓ CDC MESSAGE FILTERING TEST PASSED"

# cleanup
pgcopydb stream cleanup

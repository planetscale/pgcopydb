#!/bin/bash

set -x
set -e

# Disable pager for psql
export PAGER=cat

# Wait for databases to be ready
pgcopydb ping

# Setup source database with extensions and data
psql -o /tmp/ddl.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Create the replication slot that captures all changes
coproc ( pgcopydb snapshot --follow )

sleep 2

# Setup replication origin and sentinel
pgcopydb stream setup

# Clone with extension filters
pgcopydb clone --filters /usr/src/pgcopydb/filters.ini

kill -TERM ${COPROC_PID} || true
wait ${COPROC_PID} || true

# Inject CDC changes to BOTH filtered and non-filtered extension tables
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# Grab current LSN as streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# Prefetch changes
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

# Enable apply
pgcopydb stream sentinel set apply

# Apply CDC changes (filters already stored in catalog from clone)
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# Verify results
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/verify.sql

# Cleanup
pgcopydb stream cleanup

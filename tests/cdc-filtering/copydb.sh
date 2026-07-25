#!/bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# make sure source and target databases are ready
pgcopydb ping

# Setup source database with multiple schemas and data
psql -o /tmp/ddl.out -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# create the replication slot that captures all the changes
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone --filters /usr/src/pgcopydb/filters.ini

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the copying is done, inject CDC changes to BOTH included and excluded schemas
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

# prefetch the changes captured in our replication slot
pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

# --- Regression check: transform-time filtering ----------------------------
# Excluded schemas/tables must never be materialized into the CDC .sql files.
# With no --dir, pgcopydb writes CDC files to
# ${XDG_DATA_HOME:-$HOME/.local/share}/pgcopydb (see cdc.dir in copydb.c).
CDCDIR="${XDG_DATA_HOME:-$HOME/.local/share}/pgcopydb"
echo "Inspecting transformed CDC SQL under ${CDCDIR}"
ls -l "${CDCDIR}"/*.sql

# fail closed if there is nothing to check (avoid a vacuous pass)
if ! ls "${CDCDIR}"/*.sql >/dev/null 2>&1; then
    echo "FAIL: no CDC .sql files found under ${CDCDIR}"
    exit 1
fi

# core assertion: zero references to any excluded schema or table
if grep -nE 'cron|excluded_schema|filtered_events' "${CDCDIR}"/*.sql; then
    echo "FAIL: excluded schema/table reference found in transformed CDC SQL"
    exit 1
fi
echo "PASS: no excluded schema/table references in transformed CDC SQL"

# transactions stay balanced even when a transaction was fully filtered
nbegin=$(cat "${CDCDIR}"/*.sql | grep -c '^BEGIN')
ncommit=$(cat "${CDCDIR}"/*.sql | grep -c '^COMMIT')
echo "BEGIN=${nbegin} COMMIT=${ncommit}"
if [ "${nbegin}" != "${ncommit}" ]; then
    echo "FAIL: BEGIN/COMMIT counts differ (${nbegin}/${ncommit})"
    exit 1
fi

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the CDC changes to the target database
# (filters are already stored in the catalog from the clone step)
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# Verify that excluded schemas do not exist and included data is correct
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/verify.sql

# cleanup
pgcopydb stream cleanup

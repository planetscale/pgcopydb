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

# now allow for replaying/catching-up changes
pgcopydb stream sentinel set apply

# now apply the CDC changes to the target database
# (filters are already stored in the catalog from the clone step)
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# Verify that excluded schemas do not exist and included data is correct
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/verify.sql

# cleanup
pgcopydb stream cleanup

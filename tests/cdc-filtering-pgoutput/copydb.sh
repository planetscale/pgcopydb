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
coproc ( pgcopydb snapshot --follow --plugin pgoutput \
                  --filters /usr/src/pgcopydb/filters.ini )

sleep 1

#
# With pgoutput the filtering happens on the source server: an excluded table
# must not be in the publication at all, so its changes are never decoded.
#
pubtables()
{
    psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
         -c "select schemaname || '.' || tablename
               from pg_publication_tables
              where pubname = 'pgcopydb'
              order by 1"
}

pubtables > /tmp/pubtables.txt
cat /tmp/pubtables.txt

for t in public.users public.orders
do
    if ! grep -qx "${t}" /tmp/pubtables.txt
    then
        echo "FAIL: ${t} is missing from the publication"
        exit 1
    fi
done

for t in cron.job_run_details cron.scheduled_jobs \
         excluded_schema.test_table public.filtered_events
do
    if grep -qx "${t}" /tmp/pubtables.txt
    then
        echo "FAIL: excluded table ${t} is in the publication"
        exit 1
    fi
done

echo "PASS: publication matches the filters"

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

pgcopydb clone --filters /usr/src/pgcopydb/filters.ini

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# inject CDC changes to BOTH included and excluded schemas
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it's going to be our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

CDCDIR="${XDG_DATA_HOME:-$HOME/.local/share}/pgcopydb"
echo "Inspecting transformed CDC SQL under ${CDCDIR}"
ls -l "${CDCDIR}"/*.sql

# fail closed if there is nothing to check (avoid a vacuous pass)
if ! ls "${CDCDIR}"/*.sql >/dev/null 2>&1; then
    echo "FAIL: no CDC .sql files found under ${CDCDIR}"
    exit 1
fi

# excluded schemas and tables must never reach the CDC files
if grep -nE 'cron|excluded_schema|filtered_events' "${CDCDIR}"/*.sql; then
    echo "FAIL: excluded schema/table reference found in transformed CDC SQL"
    exit 1
fi
echo "PASS: no excluded schema/table references in transformed CDC SQL"

# the same must hold for the raw JSON, since pgoutput filters server-side and
# the excluded rows should never have been decoded in the first place
if ls "${CDCDIR}"/*.json >/dev/null 2>&1; then
    if grep -nE 'cron|excluded_schema|filtered_events' "${CDCDIR}"/*.json; then
        echo "FAIL: excluded table reached the decoder, publication filter failed"
        exit 1
    fi
    echo "PASS: excluded tables never reached the decoder"
fi

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

pgcopydb stream catchup --resume --endpos "${lsn}" -vv

# Verify that excluded schemas do not exist and included data is correct
psql -d ${PGCOPYDB_TARGET_PGURI} -f /usr/src/pgcopydb/verify.sql

users=`psql -At -d ${PGCOPYDB_TARGET_PGURI} -c 'select count(*) from public.users'`
orders=`psql -At -d ${PGCOPYDB_TARGET_PGURI} -c 'select count(*) from public.orders'`

if [ "${users}" != "5" ] || [ "${orders}" != "2" ]
then
    echo "FAIL: expected 5 users and 2 orders, found ${users} and ${orders}"
    exit 1
fi

echo "PASS: included tables replayed correctly"

pgcopydb stream cleanup

#
# Second phase: the include-only filter takes a different branch when the
# publication table list is built, so cover it too. Only the publication is
# checked here, the replay path is already covered above.
#
pgcopydb snapshot --follow --plugin pgoutput --dir /tmp/pgo-include \
         --filters /usr/src/pgcopydb/include-only.ini &
SNAPSHOT_PID=$!

sleep 3

pubtables > /tmp/pubtables2.txt
cat /tmp/pubtables2.txt

if ! grep -qx "public.users" /tmp/pubtables2.txt
then
    echo "FAIL: include-only filter dropped public.users"
    exit 1
fi

if [ `wc -l < /tmp/pubtables2.txt` != "1" ]
then
    echo "FAIL: include-only filter should publish exactly one table"
    exit 1
fi

echo "PASS: include-only filter builds the right publication"

kill -TERM ${SNAPSHOT_PID} || true
wait ${SNAPSHOT_PID} || true

pgcopydb stream cleanup --dir /tmp/pgo-include

echo "pgoutput filtering test passed"

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

# create a simple test table
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "CREATE TABLE test_data (id serial primary key, val text)"

# create the replication slot that captures all the changes
coproc ( pgcopydb snapshot --follow )

sleep 1

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

# pgcopydb clone uses the environment variables
pgcopydb clone

kill -TERM ${COPROC_PID}
wait ${COPROC_PID} || true

# inject enough data to produce multiple WAL segments worth of CDC files,
# comfortably larger than the prune threshold used below
for i in $(seq 1 200); do
    psql -d ${PGCOPYDB_SOURCE_PGURI} -c \
        "INSERT INTO test_data (val) SELECT md5(random()::text) FROM generate_series(1, 200)"
done

# allow applying/replaying changes; the follow below streams live (no endpos)
pgcopydb stream sentinel set apply

CDCDIR=/tmp/pgcopydb/cdc
followlog=/tmp/prune-follow.log

# Run follow in the background with a small prune threshold, a short min-age,
# and a short prune cycle. --not-consistent lets follow proceed without the
# snapshot exported (and since released) during the setup phase. As apply
# advances the replay LSN past the injected WAL, the watchdog (every 2s) should
# delete the applied .json/.sql files once their total exceeds the threshold.
PGCOPYDB_CDC_PRUNE_CYCLE_SECONDS=2 \
pgcopydb follow --resume --not-consistent \
    --prune-threshold 64kB \
    --prune-min-age 1s \
    -vv > "${followlog}" 2>&1 &
follow_pid=$!

# Poll (up to ~90s) for the watchdog to report deleting applied CDC files.
pruned=0
for i in $(seq 1 45); do
    if grep -qE "CDC prune: deleted [1-9][0-9]* files, freed [1-9]" "${followlog}"; then
        pruned=1
        break
    fi

    # stop early if the follow process exited on its own
    kill -0 ${follow_pid} 2>/dev/null || break

    sleep 2
done

# Stop the background follow (and its subprocesses) before asserting / tearing
# down. follow does not reliably exit on a single SIGTERM, so escalate to
# SIGKILL and do not block on wait (which would hang).
kill -TERM ${follow_pid} 2>/dev/null || true
for i in $(seq 1 5); do
    kill -0 ${follow_pid} 2>/dev/null || break
    sleep 1
done
pkill -KILL -P ${follow_pid} 2>/dev/null || true
kill -KILL ${follow_pid} 2>/dev/null || true

# let the replication slot go inactive so stream cleanup can drop it
sleep 3

echo "=== prune watchdog log lines ==="
grep "CDC prune" "${followlog}" || true

remaining=$(find ${CDCDIR} -name '*.json' -o -name '*.sql' 2>/dev/null | wc -l | tr -d ' ')
echo "Remaining CDC files after pruning: ${remaining}"

# Assert the watchdog actually deleted applied CDC files, freeing a non-zero
# number of files/bytes (not just that follow ran with the flags enabled).
if [ "${pruned}" != "1" ]; then
    echo "ERROR: prune watchdog did not delete any applied CDC files"
    echo "--- follow log tail ---"
    tail -60 "${followlog}" || true
    exit 1
fi

echo "CDC prune integration test passed"

# verify the stream cleanup teardown command still works
pgcopydb stream cleanup

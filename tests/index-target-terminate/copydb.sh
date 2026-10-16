#!/bin/bash

set -x
set -e

export PAGER=cat

DIR=/tmp/index-target-terminate

pgcopydb ping

psql -v ON_ERROR_STOP=1 -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/ddl.sql

psql -v ON_ERROR_STOP=1 -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
CREATE FUNCTION slow_idx() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_query() ~ 'idx_slow_val'
  THEN
    PERFORM pg_sleep(60);
  END IF;
END
$$;

CREATE EVENT TRIGGER slow_idx ON ddl_command_start
  WHEN TAG IN ('CREATE INDEX')
  EXECUTE FUNCTION slow_idx();
EOF

rm -rf "${DIR}"

pgcopydb clone --dir "${DIR}" --notice > /tmp/clone.log 2>&1 &
CLONE_PID=$!

TERMINATED=0

for i in $(seq 1 60)
do
    TERMINATED=$(psql -tA -d "${PGCOPYDB_TARGET_PGURI}" -c "
        SELECT count(pg_terminate_backend(pid))
          FROM pg_stat_activity
         WHERE pid <> pg_backend_pid()
           AND query LIKE 'CREATE INDEX idx_slow_val%'")

    if [ "${TERMINATED}" -ge 1 ]
    then
        break
    fi

    sleep 1
done

if wait ${CLONE_PID}
then
    cat /tmp/clone.log
    echo "clone was expected to fail"
    exit 1
fi

cat /tmp/clone.log

test "${TERMINATED}" -ge 1

grep -q "Target server is up since" /tmp/clone.log
grep -q "CREATE INDEX memory on the target: 2 processes" /tmp/clone.log
grep -q "Lost the target connection" /tmp/clone.log
grep -q "Target server did not restart" /tmp/clone.log

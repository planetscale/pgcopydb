#!/bin/bash

set -x
set -e

export PAGER=cat

DIR=/tmp/resume-vacuum

pgcopydb ping

psql -v ON_ERROR_STOP=1 -d "${PGCOPYDB_SOURCE_PGURI}" -f /usr/src/pgcopydb/ddl.sql

psql -v ON_ERROR_STOP=1 -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
CREATE FUNCTION block_idx() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('test.block_index', true) = 'on'
     AND current_query() ~ 'idx_fail_val'
  THEN
    RAISE EXCEPTION 'CREATE INDEX idx_fail_val blocked by test';
  END IF;
END
$$;

CREATE EVENT TRIGGER block_idx ON ddl_command_start
  WHEN TAG IN ('CREATE INDEX')
  EXECUTE FUNCTION block_idx();

ALTER DATABASE postgres SET test.block_index = 'on';
EOF

rm -rf "${DIR}"

if pgcopydb clone --dir "${DIR}" --notice > /tmp/clone.log 2>&1
then
    cat /tmp/clone.log
    echo "first clone was expected to fail"
    exit 1
fi

cat /tmp/clone.log

grep -q "Failed to create index" /tmp/clone.log
grep -q "VACUUM ANALYZE public.t_ok" /tmp/clone.log

psql -v ON_ERROR_STOP=1 -d "${PGCOPYDB_TARGET_PGURI}" \
     -c "ALTER DATABASE postgres RESET test.block_index"

pgcopydb clone --dir "${DIR}" --resume --not-consistent --notice \
    > /tmp/resume.log 2>&1 || { cat /tmp/resume.log; exit 1; }

cat /tmp/resume.log

if grep -q "constraint failed" /tmp/resume.log
then
    echo "resume hit a SQLite constraint failure"
    exit 1
fi

COUNT=$(psql -tA -d "${PGCOPYDB_TARGET_PGURI}" \
     -c "SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_fail_val'")
test "${COUNT}" -eq 1

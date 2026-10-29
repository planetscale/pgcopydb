#!/bin/bash

set -x
set -e

export PAGER=cat

pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

SOURCE_POLICY_COUNT=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "SELECT count(*) FROM pg_policy;")
echo "=== Source has $SOURCE_POLICY_COUNT policies naming role report_reader ==="

echo "=== Test 1: Clone with --skip-row-security, role missing on target ==="
pgcopydb clone --skip-row-security --restore-tolerance 0 --source ${PGCOPYDB_SOURCE_PGURI} --target ${PGCOPYDB_TARGET_PGURI}

POLICY_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM pg_policy;")
if [ "$POLICY_COUNT" -eq "0" ]; then
    echo "✓ Policies were skipped (count: $POLICY_COUNT)"
else
    echo "✗ Policies were NOT skipped (count: $POLICY_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT polname, polrelid::regclass FROM pg_policy;"
    exit 1
fi

RLS_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "
    SELECT count(*)
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relrowsecurity;")
if [ "$RLS_COUNT" -eq "0" ]; then
    echo "✓ Row level security was not enabled on target tables"
else
    echo "✗ Row level security is enabled on $RLS_COUNT target tables"
    exit 1
fi

ROW_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM invoices;")
if [ "$ROW_COUNT" -eq "3" ]; then
    echo "✓ Table data was copied"
else
    echo "✗ Table data was not copied (expected: 3, got: $ROW_COUNT)"
    exit 1
fi

psql -d ${PGCOPYDB_TARGET_PGURI} << 'SQL'
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS audit_notes CASCADE;
DROP FUNCTION IF EXISTS audit_notes_touch();
CREATE ROLE report_reader NOLOGIN;
SQL

rm -rf /tmp/pgcopydb

echo "=== Test 2: Clone without --skip-row-security, role exists on target ==="
pgcopydb clone --restore-tolerance 0 --source ${PGCOPYDB_SOURCE_PGURI} --target ${PGCOPYDB_TARGET_PGURI}

POLICY_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM pg_policy;")
if [ "$POLICY_COUNT" -eq "$SOURCE_POLICY_COUNT" ]; then
    echo "✓ Policies were copied (count: $POLICY_COUNT)"
else
    echo "✗ Policies were not copied (expected: $SOURCE_POLICY_COUNT, got: $POLICY_COUNT)"
    exit 1
fi

RLS_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "
    SELECT count(*)
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public' AND c.relrowsecurity;")
if [ "$RLS_COUNT" -eq "3" ]; then
    echo "✓ Row level security was enabled on target tables"
else
    echo "✗ Row level security was not enabled (expected: 3, got: $RLS_COUNT)"
    exit 1
fi

POLICY_COMMENT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "
    SELECT obj_description(oid, 'pg_policy')
      FROM pg_policy
     WHERE polname = 'accounts_read_1';")
if [ "$POLICY_COMMENT" = "read access for reports" ]; then
    echo "✓ Policy comment was copied"
else
    echo "✗ Policy comment was not copied (got: $POLICY_COMMENT)"
    exit 1
fi

echo "✓ SKIP-ROW-SECURITY TEST PASSED"

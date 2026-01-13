#!/bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI

# make sure source and target databases are ready
pgcopydb ping

# Create test tables on source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Test 1: Clone WITH --skip-vacuum
echo "=== Test 1: Clone with --skip-vacuum ==="
pgcopydb clone \
    --skip-vacuum \
    --source ${PGCOPYDB_SOURCE_PGURI} \
    --target ${PGCOPYDB_TARGET_PGURI} \
    > /tmp/test1.log 2>&1

# Verify VACUUM ANALYZE was skipped
if grep -q "skipping VACUUM jobs per --skip-vacuum" /tmp/test1.log; then
    echo "✓ VACUUM was correctly skipped"
else
    echo "✗ VACUUM skip message not found in log"
    exit 1
fi

# Check that VACUUM time is 0 (no vacuum ran)
if grep -q "VACUUM (cumulative).*0ms" /tmp/test1.log; then
    echo "✓ VACUUM time is 0ms (confirmed no vacuum ran)"
else
    echo "✗ VACUUM may have run (non-zero time)"
    grep "VACUUM (cumulative)" /tmp/test1.log || true
    exit 1
fi

# Verify tables were still copied
TABLE_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM users;")
if [ "$TABLE_COUNT" -eq "1000" ]; then
    echo "✓ Tables were copied successfully (count: $TABLE_COUNT)"
else
    echo "✗ Tables were not copied correctly (expected: 1000, got: $TABLE_COUNT)"
    exit 1
fi

# Clean target for test 2
psql -d ${PGCOPYDB_TARGET_PGURI} << 'SQL'
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
SQL

# Remove work directory to force fresh clone
rm -rf /tmp/pgcopydb

# Test 2: Clone WITHOUT --skip-vacuum (VACUUM ANALYZE should run)
echo "=== Test 2: Clone without --skip-vacuum ==="
pgcopydb clone \
    --source ${PGCOPYDB_SOURCE_PGURI} \
    --target ${PGCOPYDB_TARGET_PGURI} \
    > /tmp/test2.log 2>&1

# Verify VACUUM ANALYZE was run (should NOT see skip message and should see VACUUM step)
if ! grep -q "skipping VACUUM" /tmp/test2.log && grep -q "STEP.*VACUUM" /tmp/test2.log; then
    echo "✓ VACUUM was run (as expected)"
else
    echo "✗ VACUUM was not run (should have run)"
    grep "VACUUM" /tmp/test2.log || echo "No VACUUM mentions found"
    exit 1
fi

# Look for actual VACUUM ANALYZE statements in the log
if grep -qi "VACUUM ANALYZE" /tmp/test2.log; then
    echo "✓ VACUUM ANALYZE statements found in log"
else
    echo "⚠ Warning: No VACUUM ANALYZE statements found (but STEP 8 was executed)"
fi

# Verify tables were copied
TABLE_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM users;")
if [ "$TABLE_COUNT" -eq "1000" ]; then
    echo "✓ Tables were copied successfully (count: $TABLE_COUNT)"
else
    echo "✗ Tables were not copied correctly (expected: 1000, got: $TABLE_COUNT)"
    exit 1
fi

echo "✓ SKIP-VACUUM TEST PASSED"

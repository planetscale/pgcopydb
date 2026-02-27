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

# Create test tables and publications on source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Verify publications exist on source
echo "=== Publications on source before clone ==="
psql -d ${PGCOPYDB_SOURCE_PGURI} -c "SELECT pubname FROM pg_publication ORDER BY pubname;"

# Test 1: Clone WITH --skip-publications
echo "=== Test 1: Clone with --skip-publications ==="
pgcopydb clone --skip-publications --source ${PGCOPYDB_SOURCE_PGURI} --target ${PGCOPYDB_TARGET_PGURI}

# Verify publications do NOT exist on target
echo "=== Publications on target after --skip-publications ==="
PUB_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM pg_publication;")
if [ "$PUB_COUNT" -eq "0" ]; then
    echo "✓ Publications were correctly skipped (count: $PUB_COUNT)"
else
    echo "✗ Publications were NOT skipped (count: $PUB_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT pubname FROM pg_publication;"
    exit 1
fi

# Verify tables were copied (just publications were skipped)
TABLE_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM users;")
if [ "$TABLE_COUNT" -gt "0" ]; then
    echo "✓ Tables were copied successfully"
else
    echo "✗ Tables were not copied"
    exit 1
fi

# Clean target AND work directory for test 2
psql -d ${PGCOPYDB_TARGET_PGURI} << 'SQL'
DROP PUBLICATION IF EXISTS pub_all_tables CASCADE;
DROP PUBLICATION IF EXISTS pub_users_only CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
SQL

# Remove work directory to force fresh clone
rm -rf /tmp/pgcopydb

# Test 2: Clone WITHOUT --skip-publications (default behavior)
echo "=== Test 2: Clone without --skip-publications ==="
pgcopydb clone --source ${PGCOPYDB_SOURCE_PGURI} --target ${PGCOPYDB_TARGET_PGURI}

# Verify publications DO exist on target
echo "=== Publications on target after normal clone ==="
PUB_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM pg_publication;")
if [ "$PUB_COUNT" -eq "2" ]; then
    echo "✓ Publications were correctly copied (count: $PUB_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT pubname FROM pg_publication ORDER BY pubname;"
else
    echo "✗ Publications were not copied correctly (expected: 2, got: $PUB_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT pubname FROM pg_publication;"
    exit 1
fi

# Verify publication content
echo "=== Verifying publication content ==="
psql -d ${PGCOPYDB_TARGET_PGURI} << 'SQL'
\dRp+ pub_all_tables
\dRp+ pub_users_only
SQL

echo "✓ SKIP-PUBLICATIONS TEST PASSED"

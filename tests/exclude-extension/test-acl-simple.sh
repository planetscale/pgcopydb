#!/bin/bash

#
# Simplified local ACL filtering test
#

set -e

WORKDIR="/tmp/pgcopydb-acl-test-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "Working directory: $WORKDIR"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    cd "$WORKDIR"
    podman-compose down -v 2>/dev/null || true
}

trap cleanup EXIT

# Create compose file with unique project name
PROJECT_NAME="acl-test-$$"
cat > compose.yaml << EOF
version: '3.8'

services:
  source:
    image: postgres:16
    container_name: ${PROJECT_NAME}_source
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10

  target:
    image: postgres:16
    container_name: ${PROJECT_NAME}_target
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10
EOF

# Create setup SQL
cat > setup-source.sql << 'EOF'
-- Create a test schema to simulate extension-owned schema
create schema test_ext;

-- Create functions
create or replace function test_ext.blue_green_get_status(param text)
returns text language plpgsql as $$
begin
    return 'status: ' || param;
end;
$$;

-- Create role and grant ACLs (THIS IS WHAT WE NEED TO FILTER)
create role test_admin;
grant usage on schema test_ext to test_admin;
grant execute on function test_ext.blue_green_get_status(text) to test_admin;
revoke execute on function test_ext.blue_green_get_status(text) from public;

-- Create regular schema (should NOT be filtered)
create schema public_data;
create table public_data.users (id serial primary key, username text);
insert into public_data.users (username) values ('alice'), ('bob');
EOF

# Create filters file
cat > filters.ini << 'EOF'
[exclude-schema]
test_ext
EOF

echo "Starting containers..."
podman-compose up -d

echo "Waiting for PostgreSQL..."
sleep 10

echo "Setting up source database..."
podman exec -i "${PROJECT_NAME}_source" psql -U postgres < setup-source.sql

echo "Checking source database..."
podman exec "${PROJECT_NAME}_source" psql -U postgres -c '\dn+' -c '\df test_ext.*'

echo ""
echo "Running pgcopydb..."
podman run --rm --network host \
    -v "$WORKDIR:/work" \
    localhost/pgcopydb:latest \
    pgcopydb clone \
        --source "postgres://postgres@${PROJECT_NAME}_source/postgres" \
        --target "postgres://postgres@${PROJECT_NAME}_target/postgres" \
        --filter /work/filters.ini \
        --dir /work/pgcopydb-work \
        --verbose \
        --notice

echo ""
echo "=== ANALYSIS ==="
echo ""

# Check if test_ext was filtered
echo "1. Checking if test_ext schema exists in target (should NOT):"
if podman exec "${PROJECT_NAME}_target" psql -U postgres -tAc "SELECT 1 FROM pg_namespace WHERE nspname = 'test_ext'" 2>/dev/null | grep -q 1; then
    echo "   ❌ FAIL: test_ext schema should NOT exist in target"
    exit 1
else
    echo "   ✓ PASS: test_ext schema correctly filtered"
fi

# Check filter.db
echo ""
echo "2. Filter table contents:"
sqlite3 /work/pgcopydb-work/schema/filter.db "SELECT kind, restore_list_name FROM filter ORDER BY kind, restore_list_name LIMIT 20;"

echo ""
echo "3. s_depend row count:"
sqlite3 /work/pgcopydb-work/schema/filter.db "SELECT COUNT(*) FROM s_depend;"

# Check pre-filtered.list for ACLs
echo ""
echo "4. Checking ACL filtering in pre-filtered.list:"
echo ""
if grep -E "ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
    echo "   Found ACL entries for test_ext:"
    grep -E "ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list

    # Check if they have ; prefix (filtered)
    if grep -E "^[0-9]+; 0 0 ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
        echo ""
        echo "   ❌ FAIL: ACLs are NOT filtered (missing ';' prefix)"
        exit 1
    elif grep -E "^;.*ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
        echo ""
        echo "   ✓ PASS: ACLs correctly filtered (have ';' prefix)"
    fi
else
    echo "   ⚠️  No ACL entries found for test_ext"
    echo "   Showing all test_ext entries:"
    grep -i "test_ext" /work/pgcopydb-work/schema/pre-filtered.list || echo "   (none found)"
fi

# Check DEFAULT ACL filtering
echo ""
echo "4b. Checking DEFAULT ACL filtering:"
if grep -E "DEFAULT ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
    echo "   Found DEFAULT ACL entries for test_ext:"
    grep -E "DEFAULT ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list
    echo ""

    # Check if they have ; prefix (filtered)
    if grep -E "^;.*DEFAULT ACL.*test_ext" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
        echo "   ✓ PASS: DEFAULT ACL properly filtered"
    else
        echo "   ❌ FAIL: DEFAULT ACL for test_ext found but NOT filtered"
        exit 1
    fi
else
    echo "   ⚠️  No DEFAULT ACL entries for test_ext (may be expected)"
fi

# Verify public schema DEFAULT ACL is NOT filtered (if it exists)
if grep -E "^[^;].*DEFAULT ACL.*public" /work/pgcopydb-work/schema/pre-filtered.list >/dev/null 2>&1; then
    echo "   ✓ PASS: DEFAULT ACL for public schema preserved"
fi

# Check logs
echo ""
echo "5. ACL filtering log messages:"
if [ -f /work/pgcopydb-work/pgcopydb.log ]; then
    grep -E "(Checking if ACL|ACL filtered|Skipping ACL|ACL restoreListName)" /work/pgcopydb-work/pgcopydb.log || echo "   (no ACL messages)"
fi

# Verify non-filtered data
echo ""
echo "6. Verifying non-filtered data:"
COUNT=$(podman exec "${PROJECT_NAME}_target" psql -U postgres -tAc "SELECT count(*) FROM public_data.users" 2>/dev/null || echo "0")
if [ "$COUNT" -eq 2 ]; then
    echo "   ✓ PASS: Non-filtered data copied correctly"
else
    echo "   ❌ FAIL: Expected 2 users, got $COUNT"
    exit 1
fi

echo ""
echo "=== TEST PASSED ==="
echo "Working directory preserved: $WORKDIR"

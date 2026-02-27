#!/bin/bash

#
# Local ACL filtering test - test that ACLs are filtered when schemas are filtered
#

set -e
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="/tmp/pgcopydb-acl-test-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "Working directory: $WORKDIR"

# Cleanup function
cleanup() {
    echo "Cleaning up containers..."
    podman-compose -f "$WORKDIR/compose.yaml" down -v 2>/dev/null || true
    echo "Working directory preserved for analysis: $WORKDIR"
}

trap cleanup EXIT

# Create compose file
cat > "$WORKDIR/compose.yaml" << 'EOF'
version: '3.8'

services:
  source:
    image: postgres:16
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  target:
    image: postgres:16
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    ports:
      - "5434:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5
EOF

# Create setup SQL
cat > "$WORKDIR/setup-source.sql" << 'EOF'
-- Create a test schema to simulate extension-owned schema
create schema test_ext;

-- Create functions in the schema
create or replace function test_ext.blue_green_get_status(param text)
returns text as $$
begin
    return 'status: ' || param;
end;
$$ language plpgsql;

create or replace function test_ext.another_function(x int)
returns int as $$
begin
    return x * 2;
end;
$$ language plpgsql;

-- Create role to grant ACLs to (simulating rdsadmin)
create role test_admin;

-- Grant ACLs on schema
grant usage on schema test_ext to test_admin;
grant create on schema test_ext to test_admin;

-- Grant ACLs on functions (THIS IS WHAT WE NEED TO FILTER)
grant execute on function test_ext.blue_green_get_status(text) to test_admin;
grant execute on function test_ext.another_function(int) to test_admin;
revoke execute on function test_ext.blue_green_get_status(text) from public;

-- Create table in the extension schema
create table test_ext.data (
    id serial primary key,
    value text
);

grant select on test_ext.data to test_admin;

insert into test_ext.data (value) values ('test1'), ('test2');

-- Create a regular schema that should NOT be filtered
create schema public_data;

create table public_data.users (
    id serial primary key,
    username text
);

insert into public_data.users (username) values ('alice'), ('bob');
EOF

# Create filters file for SCHEMA filtering (to test ACL filtering logic)
cat > "$WORKDIR/filters.ini" << 'EOF'
[exclude-schema]
test_ext
EOF

# Start containers
echo "Starting PostgreSQL containers..."
podman-compose -f "$WORKDIR/compose.yaml" up -d

# Wait for health
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if podman exec pgcopydb-acl-test-$(date +%Y%m%d-%H%M%S | head -c 18)_source_1 pg_isready -U postgres >/dev/null 2>&1; then
        echo "PostgreSQL is ready"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Get actual container names
SOURCE_CONTAINER=$(podman ps --filter "label=com.docker.compose.service=source" --filter "label=com.docker.compose.project=pgcopydb-acl-test-$(basename $WORKDIR | cut -d- -f4-)" --format "{{.Names}}" | head -1)
TARGET_CONTAINER=$(podman ps --filter "label=com.docker.compose.service=target" --filter "label=com.docker.compose.project=pgcopydb-acl-test-$(basename $WORKDIR | cut -d- -f4-)" --format "{{.Names}}" | head -1)

if [ -z "$SOURCE_CONTAINER" ] || [ -z "$TARGET_CONTAINER" ]; then
    echo "ERROR: Could not find containers"
    podman ps -a
    exit 1
fi

echo "Source container: $SOURCE_CONTAINER"
echo "Target container: $TARGET_CONTAINER"

# Setup source database
echo "Setting up source database..."
podman exec -i "$SOURCE_CONTAINER" psql -U postgres < "$WORKDIR/setup-source.sql"

# List source schemas
echo "Schemas in source:"
podman exec "$SOURCE_CONTAINER" psql -U postgres -c '\dn+'

# List functions in test_ext
echo "Functions in test_ext schema:"
podman exec "$SOURCE_CONTAINER" psql -U postgres -c '\df test_ext.*'

# Show ACL grants
echo "ACLs in source:"
podman exec "$SOURCE_CONTAINER" psql -U postgres -c "SELECT schemaname, tablename, tableowner FROM pg_tables WHERE schemaname = 'test_ext'"
podman exec "$SOURCE_CONTAINER" psql -U postgres -c "SELECT n.nspname as schema, p.proname as function, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'test_ext'"

# Run pgcopydb clone with filtering
echo "Running pgcopydb clone with schema filter..."
SOURCE_URI="postgres://postgres@localhost:5433/postgres"
TARGET_URI="postgres://postgres@localhost:5434/postgres"

podman run --rm --network host \
    -v "$WORKDIR:/work" \
    localhost/pgcopydb:latest \
    pgcopydb clone \
        --source "$SOURCE_URI" \
        --target "$TARGET_URI" \
        --filter /work/filters.ini \
        --dir /work/pgcopydb-work \
        --verbose

echo ""
echo "=== ANALYSIS ==="
echo ""

# Check if schema was filtered
echo "1. Checking if test_ext schema exists in target (should NOT):"
if podman exec "$(podman ps -qf name=target)" psql -U postgres -tAc "SELECT 1 FROM pg_namespace WHERE nspname = 'test_ext'" | grep -q 1; then
    echo "   ❌ FAIL: test_ext schema should NOT exist in target"
    exit 1
else
    echo "   ✓ PASS: test_ext schema correctly filtered"
fi

# Check if functions were filtered
echo ""
echo "2. Checking if functions were filtered (should NOT exist in target):"
FUNC_COUNT=$(podman exec "$(podman ps -qf name=target)" psql -U postgres -tAc "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = 'test_ext'")
if [ "$FUNC_COUNT" -gt 0 ]; then
    echo "   ❌ FAIL: Functions should NOT exist in target"
    exit 1
else
    echo "   ✓ PASS: Functions correctly filtered"
fi

# Check filter.db to see what was filtered
echo ""
echo "3. Checking SQLite filter.db contents:"
echo ""
echo "   Filter table contents:"
sqlite3 "$WORKDIR/pgcopydb-work/schema/filter.db" "SELECT kind, restore_list_name FROM filter ORDER BY kind, restore_list_name;" | head -20

echo ""
echo "   s_depend table row count:"
sqlite3 "$WORKDIR/pgcopydb-work/schema/filter.db" "SELECT COUNT(*) FROM s_depend;"

# Check pre-filtered.list for ACL entries
echo ""
echo "4. Checking pre-filtered.list for ACL filtering:"
echo ""
echo "   ACL entries in pre-filtered.list:"
grep -E "ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" || echo "   (no ACL entries found for test_ext)"

echo ""
echo "   Checking if ACLs for test_ext are marked as filtered (have ';' prefix):"
if grep -E "^[0-9]+; 0 0 ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
    echo "   ⚠️  FOUND UNFILTERED ACLs for test_ext (missing ';' prefix)"
    grep -E "^[0-9]+; 0 0 ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list"
    echo ""
    echo "   ❌ FAIL: ACLs should be filtered (have ';' prefix)"
    exit 1
elif grep -E "^;.*ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
    echo "   ✓ PASS: ACLs correctly filtered (have ';' prefix)"
    grep -E "^;.*ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list"
else
    echo "   ⚠️  No ACL entries found for test_ext in pre-filtered.list"
    echo "   This might mean ACLs were not generated, checking full file:"
    grep -i "test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" | head -20
fi

# Check for DEFAULT ACL filtering
echo ""
echo "4b. Checking DEFAULT ACL filtering:"
echo ""
if grep -E "DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
	echo "   Found DEFAULT ACL entries for test_ext:"
	grep -E "DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list"
	echo ""

	# Check if they have ; prefix (filtered)
	if grep -E "^;.*DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
		echo "   ✓ PASS: DEFAULT ACL properly filtered (have ';' prefix)"
	else
		echo "   ❌ FAIL: DEFAULT ACL for test_ext found but NOT filtered (missing ';' prefix)"
		exit 1
	fi
else
	echo "   ⚠️  No DEFAULT ACL entries for test_ext found (may be expected if not created)"
fi

# Verify public schema DEFAULT ACL is NOT filtered (if it exists)
if grep -E "DEFAULT ACL.*public" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
	if grep -E "^[^;].*DEFAULT ACL.*public" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
		echo "   ✓ PASS: DEFAULT ACL for public schema preserved (not filtered)"
	else
		echo "   ⚠️  DEFAULT ACL for public found but appears filtered"
	fi
fi

# Check logs for ACL filtering messages
echo ""
echo "5. Checking pgcopydb logs for ACL filtering:"
if [ -f "$WORKDIR/pgcopydb-work/pgcopydb.log" ]; then
    echo ""
    echo "   ACL filtering log messages:"
    grep -E "(Checking if ACL|ACL filtered|Skipping ACL|ACL restoreListName)" "$WORKDIR/pgcopydb-work/pgcopydb.log" | head -20 || echo "   (no ACL filtering messages found)"
else
    echo "   (log file not found)"
fi

# Verify target database is functional
echo ""
echo "6. Verifying target database has non-filtered data:"
USERS_COUNT=$(podman exec "$(podman ps -qf name=target)" psql -U postgres -tAc "SELECT count(*) FROM public_data.users")
if [ "$USERS_COUNT" -eq 2 ]; then
    echo "   ✓ PASS: Non-filtered data (public_data.users) copied correctly"
else
    echo "   ❌ FAIL: Expected 2 users, got $USERS_COUNT"
    exit 1
fi

echo ""
echo "=== TEST PASSED ==="
echo ""
echo "Working directory preserved for analysis: $WORKDIR"

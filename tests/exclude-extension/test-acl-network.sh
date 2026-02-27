#!/bin/bash

#
# Local ACL filtering test using docker network
#

set -e

WORKDIR="$HOME/.pgcopydb-test/acl-network-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "Working directory: $WORKDIR"

# Cleanup
cleanup() {
    echo "Cleaning up..."
    podman-compose -f "$WORKDIR/compose.yaml" down -v 2>/dev/null || true
}

trap cleanup EXIT

# Create compose file
cat > compose.yaml << 'EOF'
version: '3.8'

services:
  source:
    image: postgres:16
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10
    networks:
      - pgcopydb-test

  target:
    image: postgres:16
    environment:
      POSTGRES_HOST_AUTH_METHOD: trust
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 10
    networks:
      - pgcopydb-test

networks:
  pgcopydb-test:
EOF

# Create setup SQL
cat > setup-source.sql << 'EOF'
create schema test_ext;

create or replace function test_ext.blue_green_get_status(param text)
returns text language plpgsql as $$
begin
    return 'status: ' || param;
end;
$$;

create role test_admin;
grant usage on schema test_ext to test_admin;
grant execute on function test_ext.blue_green_get_status(text) to test_admin;
revoke execute on function test_ext.blue_green_get_status(text) from public;

create schema public_data;
create table public_data.users (id serial primary key, username text);
insert into public_data.users (username) values ('alice'), ('bob');
EOF

# Create filters
cat > filters.ini << 'EOF'
[exclude-schema]
test_ext
EOF

echo "Starting containers..."
podman-compose up -d
sleep 10

echo "Setting up source..."
podman-compose exec -T source psql -U postgres < setup-source.sql

echo "Verifying source setup..."
podman-compose exec -T source bash -c "PAGER=cat psql -U postgres -c '\dn' -c '\df test_ext.*'"

echo ""
echo "Running pgcopydb..."

# Run pgcopydb in a container on the same network
# Create work directory first
mkdir -p "$WORKDIR/pgcopydb-work"
chmod 777 "$WORKDIR/pgcopydb-work"

# Get the network name created by compose
NETWORK_NAME="$(basename $WORKDIR)_pgcopydb-test"
echo "Using network: $NETWORK_NAME"

podman run --rm \
    --network "$NETWORK_NAME" \
    -v "$WORKDIR:/work" \
    --user root \
    localhost/pgcopydb:latest \
    pgcopydb clone \
        --source "postgres://postgres@source/postgres" \
        --target "postgres://postgres@target/postgres" \
        --filter /work/filters.ini \
        --dir /work/pgcopydb-work \
        --verbose \
        --notice

echo ""
echo "=== ANALYSIS ==="
echo ""

# Check if schema was filtered
echo "1. Schema filtering:"
if podman-compose exec -T target psql -U postgres -tAc "SELECT 1 FROM pg_namespace WHERE nspname = 'test_ext'" | grep -q 1; then
    echo "   ❌ FAIL: test_ext should NOT exist"
    exit 1
else
    echo "   ✓ PASS: test_ext correctly filtered"
fi

# Check filter contents
echo ""
echo "2. Filter table (first 20 rows):"
sqlite3 "$WORKDIR/pgcopydb-work/schema/filter.db" "SELECT kind, restore_list_name FROM filter ORDER BY kind, restore_list_name LIMIT 20;"

echo ""
echo "3. s_depend row count:"
sqlite3 "$WORKDIR/pgcopydb-work/schema/filter.db" "SELECT COUNT(*) FROM s_depend;"

# Check ACL filtering
echo ""
echo "4. ACL entries for test_ext in pre-filtered.list:"
if grep -E "ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
    grep -E "ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list"

    echo ""
    if grep -E "^[0-9]+; 0 0 ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
        echo "   ❌ FAIL: ACLs NOT filtered (no ';' prefix)"
        exit 1
    elif grep -E "^;.*ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
        echo "   ✓ PASS: ACLs correctly filtered (';' prefix)"
    fi
else
    echo "   ⚠️  No ACL entries found"
    echo "   All test_ext entries:"
    grep -i "test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" || echo "   (none)"
fi

# Check DEFAULT ACL filtering
echo ""
echo "4b. DEFAULT ACL entries for test_ext:"
if grep -E "DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
    grep -E "DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list"

    echo ""
    if grep -E "^;.*DEFAULT ACL.*test_ext" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
        echo "   ✓ PASS: DEFAULT ACL correctly filtered"
    else
        echo "   ❌ FAIL: DEFAULT ACL NOT filtered"
        exit 1
    fi
else
    echo "   ⚠️  No DEFAULT ACL entries (may be expected)"
fi

# Verify public schema DEFAULT ACL is NOT filtered (if it exists)
if grep -E "^[^;].*DEFAULT ACL.*public" "$WORKDIR/pgcopydb-work/schema/pre-filtered.list" >/dev/null 2>&1; then
    echo "   ✓ PASS: DEFAULT ACL for public preserved"
fi

# Check logs
echo ""
echo "5. ACL filtering in logs:"
grep -E "(Checking if ACL|ACL restoreListName|Skipping ACL)" "$WORKDIR/pgcopydb-work/pgcopydb.log" 2>/dev/null | head -20 || echo "   (no ACL messages)"

# Verify data
echo ""
echo "6. Non-filtered data:"
COUNT=$(podman-compose exec -T target psql -U postgres -tAc "SELECT count(*) FROM public_data.users")
if [ "$COUNT" -eq 2 ]; then
    echo "   ✓ PASS: Data copied correctly ($COUNT rows)"
else
    echo "   ❌ FAIL: Expected 2, got $COUNT"
    exit 1
fi

echo ""
echo "=== TEST PASSED ==="
echo "Workdir: $WORKDIR"

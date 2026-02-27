#!/bin/bash

#
# pgcopydb test/exclude-extension/test.sh
#
# Test extension filtering with exclude-extension section
#

set -e
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}"

# Source database connection
SOURCE_URI="postgres://postgres@source/postgres"
TARGET_URI="postgres://postgres@target/postgres"

# Clean up function
cleanup() {
    echo "Cleaning up..."
    docker-compose -f "${TEST_DIR}/docker-compose.yml" down -v || true
}

trap cleanup EXIT

# Start PostgreSQL containers
docker-compose -f "${TEST_DIR}/docker-compose.yml" up -d

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Setup source database
psql "${SOURCE_URI}" -f "${TEST_DIR}/setup-source.sql"

# List extensions in source
echo "Extensions in source database:"
psql "${SOURCE_URI}" -c '\dx'

# Run pgcopydb with extension filter
pgcopydb clone \
    --source "${SOURCE_URI}" \
    --target "${TARGET_URI}" \
    --filter "${TEST_DIR}/filters.ini" \
    --verbose

# Verify target database
echo "Extensions in target database:"
psql "${TARGET_URI}" -c '\dx'

echo "Tables in target database:"
psql "${TARGET_URI}" -c '\dt public.*'

# Check that pgcrypto was NOT copied
if psql "${TARGET_URI}" -tAc "SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'" | grep -q 1; then
    echo "ERROR: pgcrypto extension should not exist in target database"
    exit 1
fi

# Check that uuid-ossp WAS copied
if ! psql "${TARGET_URI}" -tAc "SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp'" | grep -q 1; then
    echo "ERROR: uuid-ossp extension should exist in target database"
    exit 1
fi

# Check that regular tables were copied
if ! psql "${TARGET_URI}" -tAc "SELECT count(*) FROM public.users" | grep -q 3; then
    echo "ERROR: users table should have 3 rows"
    exit 1
fi

if ! psql "${TARGET_URI}" -tAc "SELECT count(*) FROM public.documents" | grep -q 2; then
    echo "ERROR: documents table should have 2 rows"
    exit 1
fi

# Check that pgcrypto-dependent table (secrets) was NOT copied
if psql "${TARGET_URI}" -tAc "SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'secrets'" | grep -q 1; then
    echo "ERROR: secrets table (uses pgcrypto functions) should not exist in target database"
    exit 1
fi

echo "Test passed successfully!"

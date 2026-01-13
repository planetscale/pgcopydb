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

# Create test tables and large objects on source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# Save the count of large objects on source
SOURCE_LO_COUNT=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "SELECT COUNT(*) FROM pg_largeobject_metadata;")
echo "Source has ${SOURCE_LO_COUNT} large objects"

if [ "$SOURCE_LO_COUNT" -eq "0" ]; then
    echo "✗ Failed to create large objects on source"
    exit 1
fi

# Test 1: Clone WITH --skip-large-objects
echo "=== Test 1: Clone with --skip-large-objects ==="
pgcopydb clone \
    --skip-large-objects \
    --source ${PGCOPYDB_SOURCE_PGURI} \
    --target ${PGCOPYDB_TARGET_PGURI} \
    > /tmp/test1.log 2>&1

# Verify large objects were skipped
TARGET_LO_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM pg_largeobject_metadata;")
echo "Target has ${TARGET_LO_COUNT} large objects after --skip-large-objects"

if [ "$TARGET_LO_COUNT" -eq "0" ]; then
    echo "✓ Large objects were correctly skipped (count: $TARGET_LO_COUNT)"
else
    echo "✗ Large objects were copied (expected: 0, got: $TARGET_LO_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT * FROM pg_largeobject_metadata;"
    exit 1
fi

# Verify table was still copied (but OID references will be NULL or invalid)
TABLE_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM documents;")
if [ "$TABLE_COUNT" -eq "4" ]; then
    echo "✓ Table was copied successfully (count: $TABLE_COUNT)"
else
    echo "✗ Table was not copied correctly (expected: 4, got: $TABLE_COUNT)"
    exit 1
fi

# Verify log shows large objects were skipped
if grep -qi "skip.*large.*object\|large.*object.*skip\|skipping.*blob" /tmp/test1.log; then
    echo "✓ Log indicates large objects were skipped"
else
    echo "⚠ Warning: Log does not explicitly mention skipping large objects (this is OK if they were actually skipped)"
fi

# Clean target for test 2
psql -d ${PGCOPYDB_TARGET_PGURI} << 'SQL'
DROP TABLE IF EXISTS documents CASCADE;
-- Also clean up any large objects that might exist
DO $$
DECLARE
    lo_oid OID;
BEGIN
    FOR lo_oid IN SELECT oid FROM pg_largeobject_metadata LOOP
        PERFORM lo_unlink(lo_oid);
    END LOOP;
END $$;
SQL

# Remove work directory to force fresh clone
rm -rf /tmp/pgcopydb

# Test 2: Clone WITHOUT --skip-large-objects (large objects should be copied)
echo "=== Test 2: Clone without --skip-large-objects ==="
pgcopydb clone \
    --source ${PGCOPYDB_SOURCE_PGURI} \
    --target ${PGCOPYDB_TARGET_PGURI} \
    > /tmp/test2.log 2>&1

# Verify large objects were copied
TARGET_LO_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM pg_largeobject_metadata;")
echo "Target has ${TARGET_LO_COUNT} large objects without --skip-large-objects"

if [ "$TARGET_LO_COUNT" -eq "$SOURCE_LO_COUNT" ]; then
    echo "✓ Large objects were copied (expected: $SOURCE_LO_COUNT, got: $TARGET_LO_COUNT)"
else
    echo "✗ Large objects were not copied correctly (expected: $SOURCE_LO_COUNT, got: $TARGET_LO_COUNT)"
    psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT * FROM pg_largeobject_metadata;"
    exit 1
fi

# Verify table was copied
TABLE_COUNT=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT COUNT(*) FROM documents;")
if [ "$TABLE_COUNT" -eq "4" ]; then
    echo "✓ Table was copied successfully (count: $TABLE_COUNT)"
else
    echo "✗ Table was not copied correctly (expected: 4, got: $TABLE_COUNT)"
    exit 1
fi

# Verify the large object data is actually accessible and matches
echo "Verifying large object content..."
SOURCE_LO_DATA=$(psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "SELECT loid, count(data) as parts, sum(length(data)) as size FROM pg_largeobject GROUP BY loid ORDER BY loid;")
TARGET_LO_DATA=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT loid, count(data) as parts, sum(length(data)) as size FROM pg_largeobject GROUP BY loid ORDER BY loid;")

# The OIDs might be different, but the structure should match (same number of objects with same sizes)
SOURCE_SIZES=$(echo "$SOURCE_LO_DATA" | cut -d'|' -f3 | sort)
TARGET_SIZES=$(echo "$TARGET_LO_DATA" | cut -d'|' -f3 | sort)

if [ "$SOURCE_SIZES" = "$TARGET_SIZES" ]; then
    echo "✓ Large object data matches between source and target"
else
    echo "✗ Large object data does not match"
    echo "Source sizes: $SOURCE_SIZES"
    echo "Target sizes: $TARGET_SIZES"
    exit 1
fi

echo "✓ SKIP-LARGE-OBJECTS TEST PASSED"

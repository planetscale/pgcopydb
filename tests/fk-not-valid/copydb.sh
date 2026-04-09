#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

#
# Create test schema on source with FK constraints.
#
# We test two scenarios:
#  1. A FK constraint with orphaned data (should be created as NOT VALID)
#  2. A FK constraint with clean data (should be created normally as VALID)
#
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'

-- Table pair with FK that will have orphaned data
CREATE TABLE profile (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text
);

CREATE TABLE upgate_transaction (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    assigned_user_id uuid
);

ALTER TABLE upgate_transaction
    ADD CONSTRAINT upgate_transaction_assigned_user_id_profile_id_fk
    FOREIGN KEY (assigned_user_id) REFERENCES profile(id) ON DELETE CASCADE;

-- Table pair with FK that has clean data (no violations)
CREATE TABLE valid_parent (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE valid_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES valid_parent(id)
);

-- Insert valid data for both scenarios
INSERT INTO profile (id, name) VALUES
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'Alice'),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'Bob');

INSERT INTO upgate_transaction (assigned_user_id) VALUES
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');

INSERT INTO valid_parent VALUES (1, 'vp1'), (2, 'vp2');
INSERT INTO valid_child VALUES (1, 1), (2, 2);

-- Create orphaned references by bypassing FK checks via session_replication_role.
-- This simulates how real orphaned data accumulates in production databases.
SET session_replication_role = 'replica';

INSERT INTO upgate_transaction (assigned_user_id) VALUES
    ('cccccccc-cccc-cccc-cccc-cccccccccccc'),
    ('dddddddd-dddd-dddd-dddd-dddddddddddd');

SET session_replication_role = 'origin';

SQL

#
# Count FK constraints and data on source before clone.
#
src_fk_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM pg_constraint WHERE contype = 'f'")

echo "=== Source has ${src_fk_count} FK constraints ==="

src_txn_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM upgate_transaction")

echo "=== Source has ${src_txn_count} upgate_transaction rows ==="

src_orphan_count=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT count(*) FROM upgate_transaction t
   WHERE NOT EXISTS (SELECT 1 FROM profile p WHERE p.id = t.assigned_user_id)")

echo "=== Source has ${src_orphan_count} orphaned rows ==="

#
# Run pgcopydb clone. This should succeed even though the source has
# orphaned FK references, because pgcopydb will automatically create
# the violated FK constraint as NOT VALID.
#
pgcopydb clone --notice

echo ""
echo "=== Clone completed, verifying results ==="
echo ""

#
# Verify all data was copied (including orphaned rows).
#
tgt_txn_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM upgate_transaction")

echo "Source transaction count: ${src_txn_count}"
echo "Target transaction count: ${tgt_txn_count}"

if [ "${tgt_txn_count}" != "${src_txn_count}" ]; then
    echo "ERROR: Transaction count mismatch!"
    exit 1
fi

#
# Verify orphaned rows exist on target.
#
tgt_orphan_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM upgate_transaction t
   WHERE NOT EXISTS (SELECT 1 FROM profile p WHERE p.id = t.assigned_user_id)")

echo "Target orphaned rows: ${tgt_orphan_count}"

if [ "${tgt_orphan_count}" != "${src_orphan_count}" ]; then
    echo "ERROR: Orphaned row count mismatch!"
    exit 1
fi

#
# Verify FK constraint with violations exists as NOT VALID.
#
not_valid=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT NOT convalidated FROM pg_constraint
   WHERE conname = 'upgate_transaction_assigned_user_id_profile_id_fk'")

echo "FK constraint NOT VALID: ${not_valid}"

if [ "${not_valid}" != "t" ]; then
    echo "ERROR: FK constraint with violations should be NOT VALID!"
    exit 1
fi

#
# Verify FK constraint with clean data is fully VALID.
#
valid_fk=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint
   WHERE conname = 'valid_child_parent_id_fkey'")

echo "Valid FK constraint VALID: ${valid_fk}"

if [ "${valid_fk}" != "t" ]; then
    echo "ERROR: FK constraint with clean data should be VALID!"
    exit 1
fi

#
# Verify total FK constraint count matches source.
#
tgt_fk_count=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM pg_constraint WHERE contype = 'f'")

echo "Source FK count: ${src_fk_count}"
echo "Target FK count: ${tgt_fk_count}"

if [ "${tgt_fk_count}" != "${src_fk_count}" ]; then
    echo "ERROR: FK constraint count mismatch!"
    exit 1
fi

#
# Verify that future writes are enforced on the NOT VALID constraint.
# Even though existing data violations are allowed, new inserts with
# bad references should fail.
#
set +e
result=$(psql -d ${PGCOPYDB_TARGET_PGURI} -c \
  "INSERT INTO upgate_transaction (assigned_user_id) VALUES ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee')" 2>&1)
rc=$?
set -e

echo "Future write enforcement test: ${result}"

if echo "${result}" | grep -q "violates foreign key constraint"; then
    echo "Future writes are correctly enforced on NOT VALID constraint."
else
    echo "ERROR: NOT VALID constraint should still enforce on new writes!"
    exit 1
fi

echo ""
echo "fk-not-valid test: PASSED"
echo ""
echo "Clone completed successfully:"
echo "  - ${tgt_txn_count} rows copied (including ${tgt_orphan_count} orphans)"
echo "  - ${tgt_fk_count} FK constraints created"
echo "  - Violated FK constraint created as NOT VALID"
echo "  - Clean FK constraint created as VALID"
echo "  - Future writes correctly enforced on NOT VALID constraint"

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
# Build a source schema with two FK shapes:
#
#  1. A pair of tables with clean data and a validating FK on source
#     (convalidated = true). With --defer-validate-fks we expect this to
#     land on the target as NOT VALID.
#
#  2. A pair of tables where the FK was added NOT VALID on source
#     (convalidated = false). It should remain NOT VALID on the target
#     regardless of the flag.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'

CREATE TABLE clean_parent (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE clean_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES clean_parent(id)
);

INSERT INTO clean_parent VALUES (1, 'p1'), (2, 'p2');
INSERT INTO clean_child VALUES (1, 1), (2, 2);

-- Source-side NOT VALID FK
CREATE TABLE legacy_parent (
    id serial PRIMARY KEY,
    name text
);

CREATE TABLE legacy_child (
    id serial PRIMARY KEY,
    parent_id integer
);

INSERT INTO legacy_parent VALUES (1, 'lp1');
INSERT INTO legacy_child VALUES (1, 1), (2, 999);

ALTER TABLE legacy_child
    ADD CONSTRAINT legacy_child_parent_id_fkey
    FOREIGN KEY (parent_id) REFERENCES legacy_parent(id)
    NOT VALID;

SQL

#
# Sanity-check the source state: clean FK is convalidated, legacy FK is not.
#
src_clean_state=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")
src_legacy_state=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'legacy_child_parent_id_fkey'")

echo "=== Source clean FK convalidated:  ${src_clean_state} (expect t) ==="
echo "=== Source legacy FK convalidated: ${src_legacy_state} (expect f) ==="

if [ "${src_clean_state}" != "t" ] || [ "${src_legacy_state}" != "f" ]; then
    echo "ERROR: source FK setup is wrong"
    exit 1
fi

#
# Run pgcopydb clone with --defer-validate-fks. Every FK should land on
# the target as NOT VALID; pgcopydb should not run any validating seqscans.
#
pgcopydb clone --notice --defer-validate-fks

echo ""
echo "=== Clone completed, verifying results ==="
echo ""

#
# Both FKs should be NOT VALID on the target.
#
tgt_clean_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")
tgt_legacy_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'legacy_child_parent_id_fkey'")

echo "Target clean FK convalidated:  ${tgt_clean_state} (expect f)"
echo "Target legacy FK convalidated: ${tgt_legacy_state} (expect f)"

if [ "${tgt_clean_state}" != "f" ]; then
    echo "ERROR: clean FK should be NOT VALID on target with --defer-validate-fks"
    exit 1
fi

if [ "${tgt_legacy_state}" != "f" ]; then
    echo "ERROR: source-NOT-VALID FK should remain NOT VALID on target"
    exit 1
fi

#
# Data should still be copied in full.
#
tgt_clean_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM clean_child")
tgt_legacy_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT count(*) FROM legacy_child")

if [ "${tgt_clean_rows}" != "2" ] || [ "${tgt_legacy_rows}" != "2" ]; then
    echo "ERROR: row count mismatch (clean=${tgt_clean_rows}, legacy=${tgt_legacy_rows})"
    exit 1
fi

#
# Future writes should still be enforced even though the constraint is
# NOT VALID — NOT VALID only skips checks against pre-existing rows.
#
set +e
result=$(psql -d ${PGCOPYDB_TARGET_PGURI} -c \
  "INSERT INTO clean_child (id, parent_id) VALUES (99, 9999)" 2>&1)
set -e

if echo "${result}" | grep -q "violates foreign key constraint"; then
    echo "Future writes correctly enforced on NOT VALID constraint."
else
    echo "ERROR: NOT VALID constraint should still enforce on new writes!"
    echo "psql output: ${result}"
    exit 1
fi

#
# A manual VALIDATE CONSTRAINT on the clean FK should succeed (data is
# clean by construction) and flip convalidated to true. This proves the
# deferred validation path works for callers that want to validate later.
#
psql -d ${PGCOPYDB_TARGET_PGURI} -c \
    "ALTER TABLE clean_child VALIDATE CONSTRAINT clean_child_parent_id_fkey"

post_validate_state=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT convalidated FROM pg_constraint WHERE conname = 'clean_child_parent_id_fkey'")

if [ "${post_validate_state}" != "t" ]; then
    echo "ERROR: VALIDATE CONSTRAINT should have set convalidated=t"
    exit 1
fi

echo ""
echo "defer-validate-fks test: PASSED"
echo ""
echo "  - Clean FK landed on target as NOT VALID per --defer-validate-fks"
echo "  - Source-NOT-VALID FK stayed NOT VALID on target"
echo "  - Data was copied fully"
echo "  - Future writes still enforced on NOT VALID constraints"
echo "  - Manual VALIDATE CONSTRAINT succeeds against clean data"

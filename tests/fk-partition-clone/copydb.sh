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
# A HASH-partitioned table whose FKs reference regular tables. Postgres clones
# each FK per partition; before the fix pgcopydb listed those clones and tried
# to re-create them after the cascade already had, flooding the log with
# "constraint already exists". This asserts a clean, correct clone.
#
psql -d ${PGCOPYDB_SOURCE_PGURI} <<'SQL'

CREATE TABLE ref_company (id uuid PRIMARY KEY);
CREATE TABLE ref_message (id uuid PRIMARY KEY);
CREATE TABLE ref_snapshot (id uuid PRIMARY KEY);

CREATE TABLE hash_recipients (
    company_id uuid NOT NULL,
    message_id uuid NOT NULL,
    recipient_key varchar NOT NULL,
    snapshot_id uuid NOT NULL,
    tag_ids uuid[] DEFAULT '{}'::uuid[] NOT NULL,
    created_at timestamp NOT NULL,
    updated_at timestamp NOT NULL,
    CONSTRAINT hash_recipients_pkey
        PRIMARY KEY (company_id, message_id, recipient_key),
    CONSTRAINT hash_recipients_snapshot_fkey
        FOREIGN KEY (snapshot_id)
        REFERENCES ref_snapshot(id) ON DELETE SET NULL,
    CONSTRAINT hash_recipients_company_fkey
        FOREIGN KEY (company_id) REFERENCES ref_company(id),
    CONSTRAINT hash_recipients_message_fkey
        FOREIGN KEY (message_id) REFERENCES ref_message(id)
)
PARTITION BY HASH (company_id);

CREATE TABLE hash_recipients_p0 PARTITION OF hash_recipients
    FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE hash_recipients_p1 PARTITION OF hash_recipients
    FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE hash_recipients_p2 PARTITION OF hash_recipients
    FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- Partitioned-index parents (ON ONLY), which also exercise the index path.
CREATE INDEX index_hash_recipients_company ON ONLY hash_recipients (company_id);
CREATE INDEX index_hash_recipients_message ON ONLY hash_recipients (message_id);

INSERT INTO ref_company SELECT gen_random_uuid() FROM generate_series(1, 5);
INSERT INTO ref_message SELECT gen_random_uuid() FROM generate_series(1, 5);
INSERT INTO ref_snapshot SELECT gen_random_uuid() FROM generate_series(1, 5);

INSERT INTO hash_recipients
    (company_id, message_id, recipient_key, snapshot_id, created_at, updated_at)
SELECT c.id, m.id, 'k' || row_number() OVER () || '@example.com', s.id,
       now(), now()
  FROM ref_company c, ref_message m, ref_snapshot s
 LIMIT 20;

SQL

src_rows=$(psql -AtX -d ${PGCOPYDB_SOURCE_PGURI} \
  -c "SELECT count(*) FROM hash_recipients")
echo "=== Source hash_recipients rows: ${src_rows} (expect 20) ==="

if [ "${src_rows}" != "20" ]; then
    echo "ERROR: source data setup is wrong"
    exit 1
fi

# Plain clone, capturing output to check for spurious FK errors.
clonelog=$(mktemp)

set +e
pgcopydb clone --notice 2>&1 | tee "${clonelog}"
clone_rc=${PIPESTATUS[0]}
set -e

echo ""
echo "=== Clone finished (exit ${clone_rc}), verifying results ==="
echo ""

if [ "${clone_rc}" != "0" ]; then
    echo "ERROR: pgcopydb clone failed (exit ${clone_rc})"
    exit 1
fi

# Regression guard: no FK clone should be re-created. Before the fix this
# printed one "constraint ... already exists" per clone (9 here); after, none.
already=$(grep -cE 'constraint .* already exists' "${clonelog}" || true)
echo "FK 'already exists' error lines: ${already} (expect 0)"

if [ "${already}" != "0" ]; then
    echo "ERROR: partition FK clones were listed and collided on recreate;"
    echo "       the conparentid filter is not being applied"
    exit 1
fi

# The target must carry the 3 top-level FKs plus the cascaded per-partition
# clones, all validated.
tgt_parent_fks=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM pg_constraint
    WHERE contype = 'f' AND conparentid = 0
      AND conrelid = 'hash_recipients'::regclass")

tgt_clone_fks=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM pg_constraint
    WHERE contype = 'f' AND conparentid <> 0")

tgt_unvalidated=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} -c \
  "SELECT count(*) FROM pg_constraint
    WHERE contype = 'f' AND NOT convalidated")

echo "Target top-level FKs:   ${tgt_parent_fks} (expect 3)"
echo "Target per-partition FK clones: ${tgt_clone_fks} (expect 9)"
echo "Target unvalidated FKs: ${tgt_unvalidated} (expect 0)"

if [ "${tgt_parent_fks}" != "3" ]; then
    echo "ERROR: expected 3 top-level FK constraints on the partitioned table"
    exit 1
fi

if [ "${tgt_clone_fks}" != "9" ]; then
    echo "ERROR: expected 9 cascaded per-partition FK clones (3 FKs x 3 parts)"
    exit 1
fi

if [ "${tgt_unvalidated}" != "0" ]; then
    echo "ERROR: all FK constraints should be validated after a plain clone"
    exit 1
fi

#
# Data must be copied in full and remain writable through the FKs.
#
tgt_rows=$(psql -AtX -d ${PGCOPYDB_TARGET_PGURI} \
  -c "SELECT count(*) FROM hash_recipients")
echo "Target hash_recipients rows: ${tgt_rows} (expect 20)"

if [ "${tgt_rows}" != "20" ]; then
    echo "ERROR: partitioned table data was not fully copied"
    exit 1
fi

echo ""
echo "=== fk-partition-clone test passed ==="

#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_SOURCE_STANDBY_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source and target databases are ready
pgcopydb ping

# sleep 5 seconds to make sure standby is ready
sleep 5

# Load schema and data on the primary (writable) source
grep -v "OWNER TO postgres" /usr/src/pagila/pagila-schema.sql > /tmp/pagila-schema.sql

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /tmp/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql
psql -o /tmp/e.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pgcopydb/extra.sql

# Wait for standby to catch up with the primary
sleep 5

# Verify the standby is indeed read-only
psql -d ${PGCOPYDB_SOURCE_STANDBY_PGURI} -c "SELECT pg_is_in_recovery();"

# ============================================================
# TEST 1: Clone from standby with EXCLUDE filters
# ============================================================
export TMPDIR=/tmp/exclude

# list the exclude filters now, and the computed dependencies
cat /usr/src/pgcopydb/exclude.ini

# list the tables that are (not) selected by the filters
pgcopydb list tables --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/exclude.ini
pgcopydb list tables --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/exclude.ini --list-skipped

# list the dependencies of objects that are not selected by the filters
pgcopydb list depends --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/exclude.ini --list-skipped

# list the sequences that are (not) selected by the filters
pgcopydb list sequences --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/exclude.ini
pgcopydb list sequences --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/exclude.ini --list-skipped

pgcopydb clone --filters /usr/src/pgcopydb/exclude.ini --skip-ext-comments --notice \
         --resume --not-consistent \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --target ${PGCOPYDB_TARGET_PGURI}

# Validate exclude filter results
mkdir -p /tmp/results

pgopts="--single-transaction --no-psqlrc --expanded"

for f in ./exclude/sql/*.sql
do
    t=`basename $f .sql`
    r=/tmp/results/${t}.out
    e=./exclude/expected/${t}.out
    psql -d "${PGCOPYDB_TARGET_PGURI}" ${pgopts} --file ./exclude/sql/$t.sql &> $r
    test -f $e || cat $r
    diff -urN $e $r || cat $e $r
    diff -urN $e $r || exit 1
done

echo "EXCLUDE filter tests on standby: PASSED"

# ============================================================
# TEST 2: Clone from standby with INCLUDE filters
# ============================================================

# Drop and recreate target database for a clean include test
psql -d ${PGCOPYDB_TARGET_PGURI} -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'pagila' AND pid <> pg_backend_pid();" || true
dropdb -U postgres -h target pagila || true
createdb -U postgres -h target pagila

export TMPDIR=/tmp/include

# list the tables that are (not) selected by the filters
pgcopydb list tables --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/include.ini
pgcopydb list tables --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --filters /usr/src/pgcopydb/include.ini --list-skipped

pgcopydb clone --filters /usr/src/pgcopydb/include.ini --skip-ext-comments --notice \
         --resume --not-consistent \
         --source ${PGCOPYDB_SOURCE_STANDBY_PGURI} \
         --target ${PGCOPYDB_TARGET_PGURI}

# Validate include filter results
for f in ./include/sql/*.sql
do
    t=`basename $f .sql`
    r=/tmp/results/${t}.out
    e=./include/expected/${t}.out
    psql -d "${PGCOPYDB_TARGET_PGURI}" ${pgopts} --file ./include/sql/$t.sql &> $r
    test -f $e || cat $r
    diff -urN $e $r || cat $e $r
    diff -urN $e $r || exit 1
done

echo "INCLUDE filter tests on standby: PASSED"
echo "All filtering-standby tests: PASSED"

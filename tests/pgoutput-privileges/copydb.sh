#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI    the unprivileged migration role
#  - PGCOPYDB_TARGET_PGURI
#  - ADMIN_PGURI              a superuser on the source database

# the migration role does not exist yet, so wait on the admin connection
for i in $(seq 1 30)
do
    psql -At -d "${ADMIN_PGURI}" -c 'select 1' && break
    sleep 1
done

psql -d "${ADMIN_PGURI}" -f /usr/src/pgcopydb/ddl.sql

#
# Phase 1: the migration role has neither CREATE on the database nor the
# ownership of the tables. pgcopydb must fail before it creates anything.
#
if pgcopydb snapshot --follow --plugin pgoutput > /tmp/phase1.log 2>&1
then
    echo "pgcopydb must fail when it cannot create the publication"
    cat /tmp/phase1.log
    exit 1
fi

cat /tmp/phase1.log

# both causes are reported, so that one run reports the whole problem
grep 'has no CREATE privilege on database' /tmp/phase1.log
grep 'does not own 3 of the 3 tables to publish' /tmp/phase1.log
grep 'owned by app_owner' /tmp/phase1.log

# the three ways out are all offered
grep 'GRANT CREATE ON DATABASE' /tmp/phase1.log
grep -- '--publication' /tmp/phase1.log
grep -- '--plugin wal2json' /tmp/phase1.log

# a failed pre-flight leaves no publication and no replication slot behind
pubcount=`psql -At -d "${ADMIN_PGURI}" -c 'select count(*) from pg_publication'`
slotcount=`psql -At -d "${ADMIN_PGURI}" \
                -c 'select count(*) from pg_replication_slots'`

if [ "${pubcount}" != "0" -o "${slotcount}" != "0" ]
then
    echo "the failed run left ${pubcount} publication(s) and ${slotcount} slot(s)"
    exit 1
fi

#
# Phase 2: grant CREATE on the database. Ownership is still missing, so
# pgcopydb must still fail, and must now report only the ownership problem.
#
psql -d "${ADMIN_PGURI}" -c 'GRANT CREATE ON DATABASE postgres TO migration_user'

if pgcopydb snapshot --follow --plugin pgoutput > /tmp/phase2.log 2>&1
then
    echo "pgcopydb must fail when it does not own the tables to publish"
    cat /tmp/phase2.log
    exit 1
fi

cat /tmp/phase2.log

grep 'does not own 3 of the 3 tables to publish' /tmp/phase2.log
! grep 'has no CREATE privilege on database' /tmp/phase2.log

#
# Phase 3: grant membership in the role that owns the tables. Ownership
# through role membership counts, so pgcopydb must now succeed.
#
psql -d "${ADMIN_PGURI}" -c 'GRANT app_owner TO migration_user'

coproc ( pgcopydb snapshot --follow --plugin pgoutput )

sleep 2

pubcount=`psql -At -d "${ADMIN_PGURI}" \
               -c "select count(*) from pg_publication where pubname = 'pgcopydb'"`

if [ "${pubcount}" != "1" ]
then
    echo "expected pgcopydb to create the publication \"pgcopydb\""
    psql -d "${ADMIN_PGURI}" -c 'select * from pg_publication'
    exit 1
fi

# the publication covers the three tables of the migration
tablecount=`psql -At -d "${ADMIN_PGURI}" \
                 -c "select count(*) from pg_publication_tables
                      where pubname = 'pgcopydb'"`

if [ "${tablecount}" != "3" ]
then
    echo "expected 3 tables in the publication, found ${tablecount}"
    psql -d "${ADMIN_PGURI}" -c 'select * from pg_publication_tables'
    exit 1
fi

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

#
# Phase 4: an explicit --publication is used as-is, with no CREATE at all.
#
psql -d "${ADMIN_PGURI}" -c 'REVOKE CREATE ON DATABASE postgres FROM migration_user'
psql -d "${ADMIN_PGURI}" -c 'REVOKE app_owner FROM migration_user'
psql -d "${ADMIN_PGURI}" -c 'DROP PUBLICATION pgcopydb'
psql -d "${ADMIN_PGURI}" -c 'CREATE PUBLICATION customer_pub FOR ALL TABLES'
psql -d "${ADMIN_PGURI}" \
     -c "select pg_drop_replication_slot('pgcopydb')
          from pg_replication_slots where slot_name = 'pgcopydb'"

# a new work directory, the previous phase recorded another publication name
coproc ( pgcopydb snapshot --follow --plugin pgoutput \
                  --publication customer_pub --dir /tmp/phase4 )

sleep 2

# pgcopydb used the given publication and created none of its own
pubcount=`psql -At -d "${ADMIN_PGURI}" \
               -c "select count(*) from pg_publication where pubname = 'pgcopydb'"`

if [ "${pubcount}" != "0" ]
then
    echo "pgcopydb must not create a publication when --publication is used"
    exit 1
fi

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

echo "pgoutput-privileges: all phases passed"

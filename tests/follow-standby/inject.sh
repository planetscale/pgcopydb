#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_PRIMARY_PGURI     (primary for DML writes and WAL queries)
#  - PGCOPYDB_SOURCE_PGURI      (standby, used by pgcopydb sentinel commands)
#  - PGCOPYDB_TARGET_PGURI

pgcopydb ping

#
# Only start injecting DML traffic on the source database when the pagila
# schema and base data set has been deployed already. Our proxy to know that
# that's the case is the existence of the pgcopydb.sentinel table on the
# source database.
#
dbfile=${TMPDIR}/pgcopydb/schema/source.db

until [ -s ${dbfile} ]
do
    sleep 1
done

#
# Inject changes from our DML file in a loop, again and again.
#
# DML writes and pg_switch_wal go to the PRIMARY since the standby is
# read-only. The WAL changes replicate to the standby and pgcopydb picks
# them up via logical decoding from the standby.
#
# We use longer sleeps than the primary-source test to give the standby
# time to receive, replay, and decode the WAL changes.
#
for i in `seq 5`
do
    psql -d ${PGCOPYDB_PRIMARY_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 2

    psql -d ${PGCOPYDB_PRIMARY_PGURI} -f /usr/src/pgcopydb/dml.sql
    sleep 2

    psql -d ${PGCOPYDB_PRIMARY_PGURI} -c 'select pg_switch_wal()'
    sleep 2
done

# Do one final DML write after the last pg_switch_wal so that the endpos
# WAL segment contains actual DML. Without this, the endpos lands in an
# empty WAL segment and pgcopydb replay can't advance through it.
psql -d ${PGCOPYDB_PRIMARY_PGURI} -f /usr/src/pgcopydb/dml.sql

# Wait for the standby to replay all WAL from the primary
sleep 5

# Grab the current LSN from the PRIMARY
lsn=`psql -At -d ${PGCOPYDB_PRIMARY_PGURI} -c 'select pg_current_wal_flush_lsn()'`

# Set endpos with the explicit LSN value (cannot use --current because that
# would call pg_current_wal_flush_lsn on the standby which fails)
pgcopydb stream sentinel set endpos ${lsn}
pgcopydb stream sentinel get

endpos=`pgcopydb stream sentinel get --endpos 2>/dev/null`

if [ ${endpos} = "0/0" ]
then
    echo "expected ${lsn} endpos, found ${endpos}"
    exit 1
fi

#
# Because we're using docker-compose --abort-on-container-exit make sure
# that the other process in the pgcopydb service is done before exiting
# here.
#
flushlsn="0/0"

while [ ${flushlsn} \< ${endpos} ]
do
    flushlsn=`pgcopydb stream sentinel get --flush-lsn 2>/dev/null`
    sleep 1
done

#
# Still give some time to the pgcopydb service to finish its processing,
# with the cleanup and all.
#
sleep 10

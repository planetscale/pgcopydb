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

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# add the tables that exercise the pgoutput decoder
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# the source database has no wal2json installed, that is the point of pgoutput
psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
     -c "select count(*) from pg_available_extensions where name = 'wal2json'"

# create the replication slot and export the snapshot
coproc ( pgcopydb snapshot --follow --plugin pgoutput )

sleep 1

# pgcopydb creates the publication itself, named after the replication slot
pubcount=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
               -c "select count(*) from pg_publication where pubname = 'pgcopydb'"`

if [ "${pubcount}" != "1" ]
then
    echo "expected pgcopydb to create the publication \"pgcopydb\""
    psql -d ${PGCOPYDB_SOURCE_PGURI} -c 'select * from pg_publication'
    exit 1
fi

# the publication must not contain the pgcopydb internal schema
badtables=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
                -c "select count(*) from pg_publication_tables
                     where pubname = 'pgcopydb' and schemaname = 'pgcopydb'"`

if [ "${badtables}" != "0" ]
then
    echo "the publication must not contain the pgcopydb schema"
    exit 1
fi

# now setup the replication origin (target) and the pgcopydb.sentinel (source)
pgcopydb stream setup

pgcopydb clone --split-tables-larger-than 200kB

kill -TERM ${COPROC_PID}
wait ${COPROC_PID}

# now that the base copy is done, inject DML changes on the source
psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/dml.sql

# grab the current LSN, it is our streaming end position
lsn=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c 'select pg_current_wal_lsn()'`

pgcopydb stream prefetch --resume --endpos "${lsn}" -vv

# allow the changes to be replayed, then apply them
pgcopydb stream sentinel set apply
pgcopydb stream catchup --resume --endpos "${lsn}" -vv

#
# Compare the source and the target. Every table below went through the
# pgoutput decoder, so any difference is a decoder bug.
#
compare()
{
    table=$1
    query=$2

    src=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} -c "${query}"`
    tgt=`psql -At -d ${PGCOPYDB_TARGET_PGURI} -c "${query}"`

    if [ "${src}" != "${tgt}" ]
    then
        echo "MISMATCH on ${table}"
        echo "  source: ${src}"
        echo "  target: ${tgt}"
        psql -d ${PGCOPYDB_SOURCE_PGURI} -c "select * from ${table} order by id"
        psql -d ${PGCOPYDB_TARGET_PGURI} -c "select * from ${table} order by id"
        exit 1
    fi

    echo "OK ${table}: ${src}"
}

# row counts and a content checksum for each table
compare pgout_full \
    "select count(*), md5(string_agg(id || '|' || coalesce(label,'') || '|'
                                     || coalesce(optional,'') || '|' || amount,
                                     ',' order by id))
       from pgout_full"

compare pgout_default \
    "select count(*), md5(string_agg(id || '|' || coalesce(label,'') || '|'
                                     || coalesce(filler,''), ',' order by id))
       from pgout_default"

# the TOASTed column must survive an UPDATE that never sent its value
compare pgout_toast \
    "select count(*), md5(string_agg(id || '|' || md5(big) || '|'
                                     || coalesce(tag,''), ',' order by id))
       from pgout_toast"

compare pgout_types \
    "select count(*), md5(string_agg(id || '|' || payload::text || '|'
                                     || encode(blob, 'hex') || '|' || flag || '|'
                                     || ts, ',' order by id))
       from pgout_types"

# both relations of the multi-table TRUNCATE must be empty then refilled
compare pgout_trunc_a \
    "select count(*), md5(string_agg(id || '|' || label, ',' order by id))
       from pgout_trunc_a"

compare pgout_trunc_b \
    "select count(*), md5(string_agg(id || '|' || label, ',' order by id))
       from pgout_trunc_b"

# the base copy tables must match too
compare rental "select count(*) from rental"
compare payment "select count(*) from payment"

# cleanup drops the publication that pgcopydb created
pgcopydb stream cleanup

pubcount=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
               -c "select count(*) from pg_publication where pubname = 'pgcopydb'"`

if [ "${pubcount}" != "0" ]
then
    echo "expected \"pgcopydb stream cleanup\" to drop the publication"
    exit 1
fi

echo "pgoutput CDC test passed"

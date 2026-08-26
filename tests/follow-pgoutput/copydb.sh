#! /bin/bash

set -x
set -e

export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

pgcopydb ping

psql -o /tmp/s.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_SOURCE_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

psql -d ${PGCOPYDB_SOURCE_PGURI} -f /usr/src/pgcopydb/ddl.sql

# a single clone --follow run, which is what a migration actually uses
pgcopydb clone --follow --plugin pgoutput

pgcopydb stream sentinel get

#
# Compare source and target for every table that went through the decoder.
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
        exit 1
    fi

    echo "OK ${table}: ${src}"
}

compare pgout_full \
    "select count(*), md5(string_agg(id || '|' || coalesce(label,'') || '|'
                                     || coalesce(optional,'') || '|' || amount,
                                     ',' order by id))
       from pgout_full"

compare pgout_default \
    "select count(*), md5(string_agg(id || '|' || coalesce(label,''), ',' order by id))
       from pgout_default"

# the TOASTed value must survive updates that never sent it
compare pgout_toast \
    "select count(*), md5(string_agg(id || '|' || md5(big) || '|'
                                     || coalesce(tag,''), ',' order by id))
       from pgout_toast"

compare pgout_types \
    "select count(*), md5(string_agg(id || '|' || payload::text || '|'
                                     || encode(blob, 'hex'), ',' order by id))
       from pgout_types"

compare rental "select count(*) from rental"
compare payment "select count(*) from payment"

# make sure the inject service has had time to see the final sentinel values
sleep 2

pgcopydb stream cleanup

pubcount=`psql -At -d ${PGCOPYDB_SOURCE_PGURI} \
               -c "select count(*) from pg_publication where pubname = 'pgcopydb'"`

if [ "${pubcount}" != "0" ]
then
    echo "expected \"pgcopydb stream cleanup\" to drop the publication"
    exit 1
fi

echo "follow pgoutput test passed"

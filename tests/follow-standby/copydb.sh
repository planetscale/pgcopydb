#! /bin/bash

set -x
set -e

# Disable pager for psql to avoid hanging in non-interactive environments
export PAGER=cat

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_PRIMARY_PGURI     (primary for schema setup)
#  - PGCOPYDB_SOURCE_PGURI      (standby for pgcopydb clone)
#  - PGCOPYDB_TARGET_PGURI
#  - PGCOPYDB_TABLE_JOBS
#  - PGCOPYDB_INDEX_JOBS

# make sure source (standby) and target databases are ready
pgcopydb ping

# wait for the standby to be fully caught up with the primary
sleep 5

# load pagila schema and data on the PRIMARY (standby replicates it)
psql -o /tmp/s.out -d ${PGCOPYDB_PRIMARY_PGURI} -1 -f /usr/src/pagila/pagila-schema.sql
psql -o /tmp/d.out -d ${PGCOPYDB_PRIMARY_PGURI} -1 -f /usr/src/pagila/pagila-data.sql

# alter the pagila schema to allow capturing DDLs without pkey
psql -d ${PGCOPYDB_PRIMARY_PGURI} -f /usr/src/pgcopydb/ddl.sql

# wait for the standby to replicate the schema and data
sleep 5

# pgcopydb clone --follow from the standby (PG16+ logical replication)
pgcopydb clone --follow --plugin wal2json

# show final sentinel values
pgcopydb stream sentinel get

# make sure the inject service has had time to see the final sentinel values
sleep 2

# NOTE: pgcopydb stream cleanup is NOT called here because it runs
# DROP SCHEMA IF EXISTS on the source, which fails on a read-only standby.
# The containers are ephemeral so cleanup is handled by docker-compose down.

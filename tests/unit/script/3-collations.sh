#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI

# Filter out OID column since OIDs vary between runs
pgcopydb list collations -q --dir /tmp/collations 2>&1 | awk '{ if (NR <= 2) print; else if (NF >= 3) { $1 = ""; print } }'

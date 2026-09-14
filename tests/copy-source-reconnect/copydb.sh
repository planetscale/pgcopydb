#! /bin/bash

set -x
set -e
set -o pipefail

export PAGER=cat

pgcopydb ping

psql -d ${PGCOPYDB_SOURCE_PGURI} <<EOF
create table copytest(id bigint primary key, payload text);

insert into copytest(id, payload)
     select i, repeat('a', 400) from generate_series(1, 400000) as i;
EOF

psql -v ON_ERROR_STOP=1 -d ${PGCOPYDB_SOURCE_PGURI} <<EOF &
set statement_timeout to '120s';

do \$\$
declare
    victim int;
begin
    loop
        perform pg_stat_clear_snapshot();

        select pid into victim
          from pg_stat_activity
         where query ilike '%copytest%to stdout%'
           and pid <> pg_backend_pid()
         limit 1;

        if victim is not null
        then
            perform pg_terminate_backend(victim);
            exit;
        end if;

        perform pg_sleep(0.02);
    end loop;
end
\$\$;
EOF
killer=$!

pgcopydb clone --notice 2>&1 | tee /tmp/clone.log

wait ${killer}

grep "COPY succeeded after" /tmp/clone.log

count=$(psql -At -d ${PGCOPYDB_TARGET_PGURI} -c 'select count(*) from copytest')

if [ "${count}" != "400000" ]
then
    echo "target table copytest has ${count} rows, expected 400000"
    exit 1
fi

pgcopydb compare data

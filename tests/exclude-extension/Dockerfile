ARG PGCOPYDB_IMAGE=pgcopydb:pg16
FROM ${PGCOPYDB_IMAGE}

USER root
WORKDIR /usr/src/pgcopydb

COPY copydb.sh /usr/src/pgcopydb/
COPY ddl.sql /usr/src/pgcopydb/
COPY dml.sql /usr/src/pgcopydb/
COPY verify.sql /usr/src/pgcopydb/
COPY filters.ini /usr/src/pgcopydb/

RUN chmod +x /usr/src/pgcopydb/copydb.sh

USER docker
WORKDIR /var/lib/postgres

CMD bash /usr/src/pgcopydb/copydb.sh

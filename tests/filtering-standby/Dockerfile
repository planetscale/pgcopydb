# that image is built by docker compose build: project-service
FROM pagila

COPY --from=pgcopydb /usr/local/bin/pgcopydb /usr/local/bin

WORKDIR /usr/src/pgcopydb
COPY ./copydb.sh copydb.sh
COPY ./exclude.ini exclude.ini
COPY ./include.ini include.ini
COPY ./extra.sql extra.sql

# validation queries
COPY ./exclude ./test/exclude
COPY ./include ./test/include

USER docker
WORKDIR /usr/src/pgcopydb/test/
CMD ["/usr/src/pgcopydb/copydb.sh"]

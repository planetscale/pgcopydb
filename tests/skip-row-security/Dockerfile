FROM pgcopydb

USER docker
WORKDIR /usr/src/pgcopydb

COPY --chmod=755 ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql

CMD ["/usr/src/pgcopydb/copydb.sh"]

FROM pgcopydb

USER docker
WORKDIR /usr/src/pgcopydb

COPY --chmod=755 ./copydb.sh copydb.sh
COPY ./ddl.sql ddl.sql
COPY ./dml.sql dml.sql
COPY ./verify.sql verify.sql
COPY ./filters.ini filters.ini

CMD ["/usr/src/pgcopydb/copydb.sh"]

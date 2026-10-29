CREATE TABLE t_ok (
    id integer PRIMARY KEY,
    val text
);

CREATE TABLE t_noidx (
    id integer,
    val text
);

CREATE TABLE t_fail (
    id integer PRIMARY KEY,
    val text
);

CREATE INDEX idx_fail_val ON t_fail(val);

INSERT INTO t_ok SELECT i, 'ok' || i FROM generate_series(1, 1000) AS i;
INSERT INTO t_noidx SELECT i, 'noidx' || i FROM generate_series(1, 1000) AS i;
INSERT INTO t_fail SELECT i, 'fail' || i FROM generate_series(1, 1000) AS i;

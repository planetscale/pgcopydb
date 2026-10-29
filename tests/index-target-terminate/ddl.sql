CREATE TABLE t_ok (
    id integer PRIMARY KEY,
    val text
);

CREATE TABLE t_slow (
    id integer PRIMARY KEY,
    val text
);

CREATE INDEX idx_slow_val ON t_slow(val);

INSERT INTO t_ok SELECT i, 'ok' || i FROM generate_series(1, 1000) AS i;
INSERT INTO t_slow SELECT i, 'slow' || i FROM generate_series(1, 1000) AS i;

-- Create a test table with an index to exercise the full COPY path
CREATE TABLE test_data (
    id serial PRIMARY KEY,
    val text
);

INSERT INTO test_data (val)
SELECT 'row-' || g FROM generate_series(1, 1000) g;

-- Create a large object so copydb_has_large_objects() returns true
SELECT lo_create(0);

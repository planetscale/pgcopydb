-- Create a table that references large objects
DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    content_oid OID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create large objects using lo_from_bytea and insert references
INSERT INTO documents (title, content_oid) VALUES
    ('Document 1', lo_from_bytea(0, 'This is the content of document 1. It contains important information.'::bytea)),
    ('Document 2', lo_from_bytea(0, 'This is the content of document 2. More important data here.'::bytea)),
    ('Document 3', lo_from_bytea(0, 'This is the content of document 3. Even more critical information.'::bytea)),
    ('Document 4', lo_from_bytea(0, 'This is the content of document 4. Final document with data.'::bytea));

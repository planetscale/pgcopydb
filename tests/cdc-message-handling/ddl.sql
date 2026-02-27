---
--- pgcopydb test/cdc-message-handling/ddl.sql
---
--- This file implements DDL for testing message filtering.

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT
);

ALTER TABLE test_table REPLICA IDENTITY FULL;

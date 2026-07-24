---
--- This file creates tables and populate the pgcopdyb.table_size cache with
--  fake sizes.
---

-- Create three tables with identical schema and data

create table table_1 (
    c_bigserial bigserial primary key,
    c_char char(580)
);

create table table_2 (
    c_bigserial bigserial primary key,
    c_char char(150)
);

create table table_3 (
    c_bigserial bigserial primary key,
    c_char char(512)
);

-- Insert 100 rows into table_1 and duplicate data in table_2, table_3 is empty.

insert into table_1 (c_char)
select
    left (md5(random()::text),
        10)
from
    generate_series(1, 100) s (i);

insert into table_2
select
    *
from
    table_1;

--
-- also create tables with names that needs double-quoting to see that our
-- partitioning queries can cope with that
--
CREATE SCHEMA IF NOT EXISTS "Sp1eCial .Char";

CREATE TABLE "Sp1eCial .Char"."source1testing"
 (
   "s0" int PRIMARY KEY,
   "s1" int NOT NULL
 );

insert into "Sp1eCial .Char"."source1testing"("s0", "s1")
select x, (x * 2) % 100000
  from generate_series(1, 10000) AS t(x);


CREATE TABLE "Sp1eCial .Char"."Tabl e.1testing"
 (
  "iD" int PRIMARY KEY,
  "regId" int,
  "status" int,
  "nA M.e" character varying(20) NOT NULL,

   CONSTRAINT "Tabl e_fk_1_testing"
  FOREIGN KEY ("iD")
   REFERENCES "Sp1eCial .Char"."source1testing"("s0")
);

insert into "Sp1eCial .Char"."Tabl e.1testing"("iD", "regId", "status", "nA M.e")
select
    "s0",
    "s0",
    random() * 100,
    'Name ' || "s0"
from
    "Sp1eCial .Char"."source1testing";

-- Create two tables with identical schema and data to test ctid split

create table table_ctid_candidate (
    c_char char(512),
    d_char char(512)
);

create table table_ctid_candidate_skip (
    c_char char(512),
    d_char char(512)
);

-- Insert 100 rows into table_ctid_candidate
insert into table_ctid_candidate (c_char, d_char)
select
    left (md5(random()::text), 10),
    left (md5(random()::text), 10)
from
    generate_series(1, 100) s (i);

insert into table_ctid_candidate_skip
select
    *
from
    table_ctid_candidate;

-- TOAST-dominant table, no integer PK: tiny heap, large out-of-line TOAST.
-- Exercises CTID split part-count based on total size (heap + TOAST). With
-- STORAGE EXTERNAL, TOAST is not compressed, so the out-of-line size is stable
-- across PG 16/17/18. Physical shape: heap ~4 pages, TOAST ~6.4 MB,
-- pg_table_size ~6.5 MB.

create table table_toast_heavy (
    id integer,          -- no PK/unique index => CTID fallback
    payload text
);
alter table table_toast_heavy alter column payload set storage external;

insert into table_toast_heavy (id, payload)
select g, repeat(md5(g::text), 400)   -- ~12.8 KB/row > 2KB => out-of-line TOAST
from generate_series(1, 500) g;

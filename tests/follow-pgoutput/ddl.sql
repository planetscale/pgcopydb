---
--- pgcopydb test/cdc-pgoutput/ddl.sql
---
--- Extra schema to exercise the pgoutput binary decoder.

begin;

--- REPLICA IDENTITY FULL sends the whole old tuple in an 'O' section.
--- A NULL column in that section must render as IS NULL in the WHERE clause.
create table pgout_full (
  id       integer primary key,
  label    text,
  optional text,
  amount   numeric(12,4)
);

alter table pgout_full replica identity full;

--- REPLICA IDENTITY DEFAULT sends a key-only 'K' section. The non-key
--- positions arrive with status 'n' as placeholders and must be skipped.
create table pgout_default (
  id     integer primary key,
  label  text,
  filler text
);

--- An unchanged TOAST value arrives with status 'u' and must be left out of
--- the UPDATE statement.
create table pgout_toast (
  id  integer primary key,
  big text,
  tag text
);

alter table pgout_toast alter column big set storage external;

--- json has no equality operator, so a REPLICA IDENTITY FULL comparison has
--- to cast both sides to text.
create table pgout_types (
  id      integer primary key,
  payload json,
  blob    bytea,
  flag    boolean,
  ts      timestamptz
);

alter table pgout_types replica identity full;

--- TRUNCATE may name several relations in a single pgoutput message.
create table pgout_trunc_a (id integer primary key, label text);
create table pgout_trunc_b (id integer primary key, label text);

commit;

--- seed rows that the base copy carries over to the target
insert into pgout_full
  select g, 'label ' || g, case when g % 3 = 0 then null else 'set ' || g end,
         (g * 1.2345)::numeric(12,4)
    from generate_series(1, 50) g;

insert into pgout_default
  select g, 'label ' || g, repeat('f', 20) from generate_series(1, 50) g;

--- incompressible payload so the value is stored out of line and TOASTed
insert into pgout_toast
  select g,
         (select string_agg(md5(random()::text), '')
            from generate_series(1, 400)),
         'tag ' || g
    from generate_series(1, 5) g;

insert into pgout_types
  select g,
         ('{"n": ' || g || '}')::json,
         decode(lpad(to_hex(g), 8, '0'), 'hex'),
         g % 2 = 0,
         '2024-01-01 00:00:00+00'::timestamptz + (g || ' hours')::interval
    from generate_series(1, 20) g;

insert into pgout_trunc_a select g, 'a' || g from generate_series(1, 10) g;
insert into pgout_trunc_b select g, 'b' || g from generate_series(1, 10) g;

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

--- float8 must round-trip exactly: under REPLICA IDENTITY FULL it is also a
--- WHERE-clause key, so a truncated value silently matches no row
create table pgout_float (
  id  bigint primary key,
  f8  double precision,
  f4  real
);

alter table pgout_float replica identity full;

--- widest numeric and integer ranges
create table pgout_numeric (
  id      bigint primary key,
  big     bigint,
  huge    numeric,
  scaled  numeric(30,15)
);

alter table pgout_numeric replica identity full;

--- text values that need JSON escaping on the way through the decoder
create table pgout_escape (
  id  integer primary key,
  t   text
);

alter table pgout_escape replica identity full;

--- identifiers that need quoting, including an embedded double quote
create schema if not exists "Sp1eCial .Char";

create table "Sp1eCial .Char"."dq""name" (
  "s0"    serial primary key,
  "c""1"  integer not null,
  "MiXeD" text
);

alter table "Sp1eCial .Char"."dq""name" replica identity full;

--- remaining common types
create table pgout_misc (
  id     integer primary key,
  u      uuid,
  d      date,
  tm     time,
  iv     interval,
  ip     inet,
  arr    integer[],
  tarr   text[],
  jb     jsonb,
  empty  text
);

alter table pgout_misc replica identity full;

insert into pgout_float (id, f8, f4) values
  (1, -216237.00000035969, 1.5),
  (2, 0.1234567890123, 0.1),
  (3, 1e-20, 1e-10),
  (4, 'Infinity', 'Infinity'),
  (5, 'NaN', 'NaN');

insert into pgout_numeric (id, big, huge, scaled) values
  (1, 9223372036854775807, 123456789012345678901234567890, 1.000000000000001),
  (2, -9223372036854775808, -0.00000000000000000001, -99999999999999.999999999999999);

insert into pgout_escape (id, t) values
  (1, 'has ''one quote'),
  (2, 'has "double" quotes'),
  (3, 'back\slash and /slash'),
  (4, E'tab\there'),
  (5, E'newline\nhere'),
  (6, E'carriage\rreturn'),
  (7, 'unicode: café 日本語'),
  (8, ''),
  (9, '{"looks": "like json"}');

insert into "Sp1eCial .Char"."dq""name" ("c""1", "MiXeD") values (1, 'a'), (2, 'b');

insert into pgout_misc (id, u, d, tm, iv, ip, arr, tarr, jb, empty) values
  (1, '11111111-2222-3333-4444-555555555555', '2024-02-29', '13:45:01.5',
      '1 year 2 mons 3 days 04:05:06', '192.168.0.1/24',
      '{1,2,3}', '{"a b","c,d","e\"f"}', '{"k": [1, 2, null]}', ''),
  (2, null, null, null, null, null, null, null, null, null);

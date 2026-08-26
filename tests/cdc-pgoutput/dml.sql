---
--- pgcopydb test/cdc-pgoutput/dml.sql
---
--- DML that exercises every branch of the pgoutput binary decoder.

begin;

--- plain INSERT, 'N' tuple only
insert into pgout_full
  select g, 'label ' || g, case when g % 3 = 0 then null else 'set ' || g end,
         (g * 1.2345)::numeric(12,4)
    from generate_series(51, 60) g;

--- UPDATE that leaves the key alone: pgoutput sends no old tuple and the
--- decoder has to synthesize the key from the new tuple.
update pgout_full set label = 'renamed' where id between 1 and 5;

--- UPDATE that changes the key: pgoutput sends an 'O' section (identity full)
update pgout_full set id = id + 1000 where id between 10 and 12;

--- DELETE against REPLICA IDENTITY FULL where a column is NULL. The WHERE
--- clause must use IS NULL for that column, otherwise nothing is deleted.
delete from pgout_full where id in (3, 6, 9);

commit;

begin;

--- REPLICA IDENTITY DEFAULT: only the key reaches the 'K' section
update pgout_default set label = 'changed' where id between 20 and 30;
delete from pgout_default where id between 40 and 45;

insert into pgout_default
  select g, 'label ' || g, repeat('g', 20) from generate_series(51, 55) g;

commit;

begin;

--- UPDATE that does not touch the TOASTed column. pgoutput reports it with
--- status 'u' and the value must stay unchanged on the target.
update pgout_toast set tag = 'updated tag' where id <= 3;

--- UPDATE that does replace the TOASTed value
update pgout_toast
   set big = (select string_agg(md5(random()::text), '')
                from generate_series(1, 400))
 where id = 4;

commit;

begin;

--- json, bytea, boolean and timestamptz round trip
update pgout_types set payload = '{"n": -1}'::json where id <= 5;
update pgout_types set flag = not flag where id between 6 and 10;
delete from pgout_types where id between 15 and 17;

insert into pgout_types
  select g,
         ('{"n": ' || g || '}')::json,
         decode(lpad(to_hex(g), 8, '0'), 'hex'),
         g % 2 = 0,
         '2024-01-01 00:00:00+00'::timestamptz + (g || ' hours')::interval
    from generate_series(21, 25) g;

commit;

--- a single TRUNCATE naming two relations produces one pgoutput message
--- carrying both relation OIDs
truncate table pgout_trunc_a, pgout_trunc_b;

insert into pgout_trunc_a select g, 'a' || g from generate_series(100, 105) g;
insert into pgout_trunc_b select g, 'b' || g from generate_series(100, 105) g;

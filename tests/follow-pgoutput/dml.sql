---
--- pgcopydb test/follow-pgoutput/dml.sql
---
--- This file runs many times during the follow test, so every statement has
--- to be safe to repeat.

begin;

update pgout_full set label = 'renamed ' || clock_timestamp() where id <= 5;

update pgout_default set label = 'changed ' || clock_timestamp()
 where id between 20 and 25;

--- leave the TOASTed column alone, it arrives with status 'u'
update pgout_toast set tag = 'tag ' || clock_timestamp() where id <= 3;

update pgout_types set payload = '{"n": -1}'::json where id <= 5;

commit;

begin;

--- pagila traffic so the base tables move as well
with r as
 (
   insert into rental(rental_date, inventory_id, customer_id, staff_id, last_update)
        select '2022-06-01', 371, 291, 1, '2022-06-01'
     returning rental_id, customer_id, staff_id
 )
 insert into payment(customer_id, staff_id, rental_id, amount, payment_date)
      select customer_id, staff_id, rental_id, 5.99, '2022-06-01'
        from r;

commit;

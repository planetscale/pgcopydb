-- verify the tables themselves exist (they're in foo, not excluded)
select count(*) as tbl_with_cross_fk_exists
  from pg_tables
 where schemaname = 'foo' and tablename = 'tbl_with_cross_fk';

-- FK constraint referencing excluded schema should NOT exist
select count(*) as cross_fk_count
  from pg_constraint c
  join pg_class cl on c.conrelid = cl.oid
  join pg_namespace n on cl.relnamespace = n.oid
 where c.contype = 'f'
   and n.nspname = 'foo'
   and cl.relname = 'tbl_with_cross_fk';

-- view referencing excluded schema should NOT exist
select count(*) as cross_view_count
  from pg_views
 where schemaname = 'foo' and viewname = 'cross_schema_view';

-- RLS table should exist
select count(*) as tbl_with_cross_rls_exists
  from pg_tables
 where schemaname = 'foo' and tablename = 'tbl_with_cross_rls';

-- RLS policy referencing excluded schema should NOT exist
select count(*) as cross_policy_count
  from pg_policy pol
  join pg_class c on pol.polrelid = c.oid
  join pg_namespace n on c.relnamespace = n.oid
 where n.nspname = 'foo' and pol.polname = 'cross_schema_policy';

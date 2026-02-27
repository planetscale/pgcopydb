-- foo.tbl2 should not exist on target (it was excluded)
select exists(
    select 1 from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'foo' and c.relname = 'tbl2'
);

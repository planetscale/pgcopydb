-- No public tables should exist on target (public schema was excluded)
select count(*)
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = 'public' and c.relkind = 'r';

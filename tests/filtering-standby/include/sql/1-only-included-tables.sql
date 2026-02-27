-- Only the 4 included tables should exist
  select n.nspname, c.relname
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where c.relkind = 'r'
     and n.nspname not in ('pg_catalog', 'information_schema')
order by n.nspname, c.relname;

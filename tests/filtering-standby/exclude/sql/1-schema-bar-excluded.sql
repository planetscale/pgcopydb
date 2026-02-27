-- Schema 'bar' should not exist on target (it was excluded)
select exists(select 1 from pg_namespace where nspname = 'bar');

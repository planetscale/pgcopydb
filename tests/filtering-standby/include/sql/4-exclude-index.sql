-- tbl1_status_id_idx should not exist (it was in exclude-index)
select exists(
    select 1 from pg_indexes
    where schemaname = 'foo'
      and tablename = 'tbl1'
      and indexname = 'tbl1_status_id_idx'
);

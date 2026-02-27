::

   pgcopydb list triggers: List all the source triggers
   usage: pgcopydb list triggers  --source ... [ --schema-name [ --table-name ] ]
   
     --source            Postgres URI to the source database
     --force             Force fetching catalogs again
     --schema-name       Filter by schema name
     --table-name        Filter by table name
     --filter <filename> Use the filters defined in <filename>
   

::

   pgcopydb snapshot: Create and export a snapshot on the source database
   usage: pgcopydb snapshot  --source ... 
   
     --source                      Postgres URI to the source database
     --dir                         Work directory to use
     --follow                      Implement logical decoding to replay changes
     --plugin                      Output plugin to use (test_decoding, wal2json, pgoutput)
     --publication                 Publication to use with the pgoutput plugin
     --filters <filename>          Use the filters defined in <filename>
     --wal2json-numeric-as-string  Print numeric data type as string when using wal2json output plugin
     --slot-name                   Use this Postgres replication slot name
   

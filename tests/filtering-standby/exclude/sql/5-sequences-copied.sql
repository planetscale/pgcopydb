-- Sequences in non-excluded schemas should be copied with their values
select last_value from seq.default_table_id_seq;
select last_value from seq.identity_table_id_seq;
select last_value from seq.standalone_id_seq;

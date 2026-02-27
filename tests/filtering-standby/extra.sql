--
-- Extra schemas and tables for filtering tests on a standby
--

create schema foo;

create table foo.tbl_status (
    id bigserial not null primary key,
    name varchar(32) not null unique check (name != '')
);

insert into foo.tbl_status (id, name)
     values (1, 'draft'),
            (2, 'active'),
            (3, 'closed');

SELECT setval(pg_get_serial_sequence('foo.tbl_status', 'id'),
              (SELECT COALESCE(MAX(id) + 1, 1) FROM foo.tbl_status),
              false);

create table foo.tbl1 (
    id bigserial not null primary key,
    status_id bigint not null default 1 references foo.tbl_status(id),
    desc_text varchar(32)
);

create index if not exists tbl1_status_id_idx on foo.tbl1(status_id);

create table foo.tbl2 (
    id bigserial not null primary key,
    tbl1_id bigint not null references foo.tbl1(id),
    desc_text varchar(32)
);

create index if not exists tbl2_tbl1_id_idx on foo.tbl2(tbl1_id);

--
-- A schema to exclude wholesale
--
create schema bar;

create table bar.should_not_exist (id serial primary key, val text);
insert into bar.should_not_exist (val) values ('this should be filtered out');

--
-- Sequences in their own schema
--
create schema seq;

create sequence seq.default_table_id_seq;
create table seq.default_table (id integer primary key default nextval('seq.default_table_id_seq'));
select setval('seq.default_table_id_seq', 667);

create table seq.identity_table (id integer primary key generated always as identity);
select setval('seq.identity_table_id_seq', 668);

create sequence seq.standalone_id_seq;
select setval('seq.standalone_id_seq', 669);

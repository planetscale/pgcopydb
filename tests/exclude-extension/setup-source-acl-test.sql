---
--- pgcopydb test/exclude-extension/setup-source-acl-test.sql
---
--- This test simulates the rds_tools scenario:
--- - Create a schema owned by an extension
--- - Create functions in that schema
--- - Grant ACLs on those functions
--- - Verify that when extension is filtered, ACLs are also filtered

-- Create regular table
create table public.users (
    id serial primary key,
    username text not null
);

insert into public.users (username) values ('alice'), ('bob');

-- Create a test extension with its own schema
-- We'll use dblink as it creates functions in public, then move them
create extension if not exists dblink;

-- Create a custom schema to simulate an extension-owned schema
create schema test_ext;

-- Create functions in the schema (simulating extension-owned functions)
create or replace function test_ext.test_function(param text)
returns text as $$
begin
    return 'result: ' || param;
end;
$$ language plpgsql;

create or replace function test_ext.another_function(x int)
returns int as $$
begin
    return x * 2;
end;
$$ language plpgsql;

-- Create a role to grant ACLs to
create role test_role;

-- Grant ACLs on schema
grant usage on schema test_ext to test_role;
grant usage on schema test_ext to public;

-- Grant ACLs on functions (this is what causes the problem)
grant execute on function test_ext.test_function(text) to test_role;
grant execute on function test_ext.another_function(int) to test_role;
revoke execute on function test_ext.test_function(text) from public;

-- Create table in the extension schema
create table test_ext.data (
    id serial primary key,
    value text
);

insert into test_ext.data (value) values ('test1'), ('test2');

-- Grant ACLs on table
grant select on test_ext.data to test_role;

-- Now create an actual extension to filter (pgcrypto)
create extension if not exists pgcrypto;

-- Create objects that depend on pgcrypto
create table public.secrets (
    id serial primary key,
    secret_hash text default gen_salt('bf')
);

insert into public.secrets (secret_hash) values (gen_salt('bf'));

-- Test DEFAULT PRIVILEGES filtering
-- These should be filtered when test_ext schema is filtered

-- Create role for default privileges
create role default_priv_role;

-- Set default privileges in extension schema (should be filtered)
ALTER DEFAULT PRIVILEGES IN SCHEMA test_ext
    GRANT SELECT ON TABLES TO default_priv_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA test_ext
    GRANT USAGE ON SEQUENCES TO default_priv_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA test_ext
    GRANT EXECUTE ON FUNCTIONS TO default_priv_role;

-- Test FOR ROLE variant
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA test_ext
    GRANT SELECT ON TABLES TO public;

-- Test non-filtered schema (should NOT be filtered)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO default_priv_role;

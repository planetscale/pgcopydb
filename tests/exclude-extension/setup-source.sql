---
--- pgcopydb test/exclude-extension/setup-source.sql
---
--- This file creates a source database with multiple extensions
--- to test extension filtering.

-- Create and populate a regular table
create table public.users (
    id serial primary key,
    username text not null,
    created_at timestamptz default now()
);

insert into public.users (username)
     values ('alice'), ('bob'), ('charlie');

-- Install uuid-ossp extension (should be kept)
create extension if not exists "uuid-ossp";

create table public.documents (
    id uuid primary key default uuid_generate_v4(),
    title text not null,
    content text
);

insert into public.documents (title, content)
     values ('Doc 1', 'Content 1'),
            ('Doc 2', 'Content 2');

-- Install pgcrypto extension (should be filtered)
create extension if not exists pgcrypto;

create table public.secrets (
    id serial primary key,
    secret_hash text,
    salt text default gen_salt('bf')
);

insert into public.secrets (secret_hash)
     values (crypt('password123', gen_salt('bf'))),
            (crypt('secret456', gen_salt('bf')));

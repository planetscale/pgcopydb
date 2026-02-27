---
--- pgcopydb test/exclude-extension/ddl.sql
---
--- Setup source database with extensions and tables
---

-- Create regular public tables (should be copied)
CREATE TABLE public.users (
    id serial primary key,
    username text not null,
    created_at timestamptz default now()
);

INSERT INTO public.users (username)
VALUES ('alice'), ('bob'), ('charlie');

-- Install uuid-ossp extension (should be kept)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public.documents (
    id uuid primary key default uuid_generate_v4(),
    title text not null,
    content text
);

INSERT INTO public.documents (title, content)
VALUES ('Doc 1', 'Content 1'),
       ('Doc 2', 'Content 2');

-- Install pgcrypto extension (should be FILTERED OUT)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create a regular table that uses pgcrypto functions
-- NOTE: This table is NOT owned by the extension (no deptype='e' in pg_depend)
-- It's a user table that happens to use extension functions, so it SHOULD be copied
CREATE TABLE public.secrets (
    id serial primary key,
    secret_data text,
    secret_hash text
);

-- Insert data using pgcrypto functions
INSERT INTO public.secrets (secret_data, secret_hash)
VALUES ('password123', crypt('password123', gen_salt('bf'))),
       ('secret456', crypt('secret456', gen_salt('bf')));

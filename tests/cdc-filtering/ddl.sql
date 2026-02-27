-- Create multiple schemas to test filtering
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS cron;
CREATE SCHEMA IF NOT EXISTS excluded_schema;

-- Create tables in public schema (should be included)
CREATE TABLE public.users (
    id serial PRIMARY KEY,
    username text NOT NULL,
    email text
);

CREATE TABLE public.orders (
    id serial PRIMARY KEY,
    user_id int REFERENCES public.users(id),
    amount numeric
);

-- Create tables in cron schema (should be excluded)
CREATE TABLE cron.job_run_details (
    id serial PRIMARY KEY,
    job_id int NOT NULL,
    run_time timestamp DEFAULT now(),
    status text
);

CREATE TABLE cron.scheduled_jobs (
    id serial PRIMARY KEY,
    job_name text NOT NULL,
    schedule text
);

-- Create tables in excluded_schema (should be excluded)
CREATE TABLE excluded_schema.test_table (
    id serial PRIMARY KEY,
    data text
);

-- Insert initial data in public schema
INSERT INTO public.users (username, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com'),
    ('charlie', 'charlie@example.com');

INSERT INTO public.orders (user_id, amount) VALUES
    (1, 100.50),
    (2, 250.75);

-- Insert initial data in excluded schemas
INSERT INTO cron.job_run_details (job_id, status) VALUES
    (1, 'completed'),
    (2, 'failed');

INSERT INTO cron.scheduled_jobs (job_name, schedule) VALUES
    ('cleanup', '0 0 * * *');

INSERT INTO excluded_schema.test_table (data) VALUES
    ('should not be copied');

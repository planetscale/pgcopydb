-- A migration role that owns no table, in the shape that managed Postgres
-- services hand out: it can read and it can stream, but it cannot create a
-- publication.
CREATE ROLE app_owner NOLOGIN;
CREATE ROLE migration_user LOGIN PASSWORD 'h4ckm3' REPLICATION;

CREATE TABLE payment(id bigint primary key, amount numeric);
CREATE TABLE invoice(id bigint primary key, payment_id bigint, label text);
CREATE TABLE ledger(id bigint primary key, entry text);

ALTER TABLE payment OWNER TO app_owner;
ALTER TABLE invoice OWNER TO app_owner;
ALTER TABLE ledger OWNER TO app_owner;

GRANT USAGE ON SCHEMA public TO migration_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO migration_user;

DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS invoices CASCADE;
DROP TABLE IF EXISTS audit_notes CASCADE;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'report_reader') THEN
        CREATE ROLE report_reader NOLOGIN;
    END IF;
END
$$;

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE invoices (
    id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(id),
    amount NUMERIC(10,2)
);

INSERT INTO accounts (name) VALUES ('alpha'), ('beta'), ('gamma');

INSERT INTO invoices (account_id, amount) VALUES (1, 10.00), (2, 20.00), (3, 30.00);

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    FOR i IN 1..6 LOOP
        EXECUTE format('CREATE POLICY accounts_read_%s ON accounts FOR SELECT TO report_reader USING (true)', i);
        EXECUTE format('CREATE POLICY invoices_read_%s ON invoices FOR SELECT TO report_reader USING (true)', i);
    END LOOP;
END
$$;

COMMENT ON POLICY accounts_read_1 ON accounts IS 'read access for reports';

CREATE TABLE audit_notes (body TEXT);
ALTER TABLE audit_notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_notes_read ON audit_notes FOR SELECT TO report_reader USING (true);
COMMENT ON POLICY audit_notes_read ON audit_notes IS 'read access for audits';

CREATE OR REPLACE FUNCTION audit_notes_touch() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;

CREATE TRIGGER audit_notes_touch BEFORE INSERT ON audit_notes
    FOR EACH ROW EXECUTE FUNCTION audit_notes_touch();

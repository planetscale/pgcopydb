-- Create test tables
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO users (username, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com'),
    ('charlie', 'charlie@example.com');

INSERT INTO orders (user_id, amount) VALUES
    (1, 99.99),
    (2, 149.99),
    (1, 29.99);

-- Create publications
DROP PUBLICATION IF EXISTS pub_all_tables CASCADE;
DROP PUBLICATION IF EXISTS pub_users_only CASCADE;

-- Publication for all tables
CREATE PUBLICATION pub_all_tables FOR ALL TABLES;

-- Publication for specific table
CREATE PUBLICATION pub_users_only FOR TABLE users;

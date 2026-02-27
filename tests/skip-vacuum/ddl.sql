-- Create test tables with enough data to make VACUUM/ANALYZE meaningful
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount NUMERIC(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert 1000 rows to make VACUUM/ANALYZE more noticeable
INSERT INTO users (username, email)
SELECT
    'user' || i,
    'user' || i || '@example.com'
FROM generate_series(1, 1000) AS i;

-- Insert 5000 orders
INSERT INTO orders (user_id, amount)
SELECT
    (random() * 999 + 1)::int,
    (random() * 1000)::numeric(10,2)
FROM generate_series(1, 5000);

-- Create indexes that will benefit from statistics
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_amount ON orders(amount);

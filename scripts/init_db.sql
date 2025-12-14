-- Initialize schemas for different purposes
-- Note: The 'airflow' database is already created by Docker environment variables

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Sample tables for demonstration
CREATE TABLE IF NOT EXISTS raw.user_events (
    event_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    transaction_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.product_catalog (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_user_events_user_id ON raw.user_events(user_id);
CREATE INDEX idx_user_events_timestamp ON raw.user_events(event_timestamp);
CREATE INDEX idx_sales_user_id ON raw.sales_transactions(user_id);
CREATE INDEX idx_sales_product_id ON raw.sales_transactions(product_id);
CREATE INDEX idx_sales_date ON raw.sales_transactions(transaction_date);

-- Insert some sample data
INSERT INTO raw.product_catalog (product_name, category, price, stock_quantity) VALUES
    ('Laptop Pro', 'Electronics', 1299.99, 50),
    ('Wireless Mouse', 'Electronics', 29.99, 200),
    ('Desk Chair', 'Furniture', 199.99, 30),
    ('Standing Desk', 'Furniture', 499.99, 15),
    ('USB-C Hub', 'Electronics', 49.99, 100);

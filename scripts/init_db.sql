-- Database Initialization Script
-- Loads Northwind sample database for data engineering demonstrations

-- Note: The 'airflow' database is created automatically by Docker environment variables

\i /docker-entrypoint-initdb.d/northwind_schema.sql

-- The Northwind schema includes:
-- - 8 Categories of products
-- - 10 Suppliers across different countries
-- - 25 Products with inventory and pricing
-- - 30 Customers across multiple countries
-- - 9 Employees with hierarchical reporting
-- - 3 Shipping companies
-- - 200 Orders from the last 90 days
-- - ~600-800 Order line items (order_details)
--
-- Perfect for demonstrating:
-- - ETL pipelines processing order data
-- - Data quality checks on transactions
-- - Customer analytics and segmentation
-- - Product performance analysis
-- - Inventory management
-- - Sales forecasting

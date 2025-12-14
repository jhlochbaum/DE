-- Northwind Database Schema for PostgreSQL
-- Classic Microsoft sample database adapted for data engineering demos
-- Domain: Import/Export company trading specialty foods

-- Drop existing tables if they exist
DROP TABLE IF EXISTS order_details CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS employees CASCADE;
DROP TABLE IF EXISTS shippers CASCADE;
DROP TABLE IF EXISTS territories CASCADE;
DROP TABLE IF EXISTS region CASCADE;

-- Create tables
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(15) NOT NULL,
    description TEXT,
    picture BYTEA
);

CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24),
    homepage TEXT
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(40) NOT NULL,
    supplier_id INT REFERENCES suppliers(supplier_id),
    category_id INT REFERENCES categories(category_id),
    quantity_per_unit VARCHAR(20),
    unit_price DECIMAL(10,2) DEFAULT 0,
    units_in_stock SMALLINT DEFAULT 0,
    units_on_order SMALLINT DEFAULT 0,
    reorder_level SMALLINT DEFAULT 0,
    discontinued BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE region (
    region_id SERIAL PRIMARY KEY,
    region_description VARCHAR(50) NOT NULL
);

CREATE TABLE territories (
    territory_id VARCHAR(20) PRIMARY KEY,
    territory_description VARCHAR(50) NOT NULL,
    region_id INT NOT NULL REFERENCES region(region_id)
);

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    last_name VARCHAR(20) NOT NULL,
    first_name VARCHAR(10) NOT NULL,
    title VARCHAR(30),
    title_of_courtesy VARCHAR(25),
    birth_date DATE,
    hire_date DATE,
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    home_phone VARCHAR(24),
    extension VARCHAR(4),
    photo BYTEA,
    notes TEXT,
    reports_to INT REFERENCES employees(employee_id),
    photo_path VARCHAR(255)
);

CREATE TABLE customers (
    customer_id VARCHAR(5) PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24)
);

CREATE TABLE shippers (
    shipper_id SERIAL PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    phone VARCHAR(24)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(5) REFERENCES customers(customer_id),
    employee_id INT REFERENCES employees(employee_id),
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via INT REFERENCES shippers(shipper_id),
    freight DECIMAL(10,2) DEFAULT 0,
    ship_name VARCHAR(40),
    ship_address VARCHAR(60),
    ship_city VARCHAR(15),
    ship_region VARCHAR(15),
    ship_postal_code VARCHAR(10),
    ship_country VARCHAR(15)
);

CREATE TABLE order_details (
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    unit_price DECIMAL(10,2) NOT NULL DEFAULT 0,
    quantity SMALLINT NOT NULL DEFAULT 1,
    discount REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (order_id, product_id)
);

-- Create indexes for performance
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_supplier ON products(supplier_id);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_employee ON orders(employee_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_shipped ON orders(shipped_date);
CREATE INDEX idx_order_details_product ON order_details(product_id);

-- Insert sample data
-- Categories
INSERT INTO categories (category_name, description) VALUES
('Beverages', 'Soft drinks, coffees, teas, beers, and ales'),
('Condiments', 'Sweet and savory sauces, relishes, spreads, and seasonings'),
('Confections', 'Desserts, candies, and sweet breads'),
('Dairy Products', 'Cheeses'),
('Grains/Cereals', 'Breads, crackers, pasta, and cereal'),
('Meat/Poultry', 'Prepared meats'),
('Produce', 'Dried fruit and bean curd'),
('Seafood', 'Seaweed and fish');

-- Suppliers
INSERT INTO suppliers (company_name, contact_name, contact_title, city, country, phone) VALUES
('Exotic Liquids', 'Charlotte Cooper', 'Purchasing Manager', 'London', 'UK', '(171) 555-2222'),
('New Orleans Cajun Delights', 'Shelley Burke', 'Order Administrator', 'New Orleans', 'USA', '(100) 555-4822'),
('Grandma Kellys Homestead', 'Regina Murphy', 'Sales Representative', 'Ann Arbor', 'USA', '(313) 555-5735'),
('Tokyo Traders', 'Yoshi Nagase', 'Marketing Manager', 'Tokyo', 'Japan', '(03) 3555-5011'),
('Cooperativa de Quesos', 'Antonio del Valle Saavedra', 'Export Administrator', 'Oviedo', 'Spain', '(98) 598 76 54'),
('Mayumis', 'Mayumi Ohno', 'Marketing Representative', 'Osaka', 'Japan', '(06) 431-7877'),
('Pavlova Ltd.', 'Ian Devling', 'Marketing Manager', 'Melbourne', 'Australia', '(03) 444-2343'),
('Specialty Biscuits Ltd.', 'Peter Wilson', 'Sales Representative', 'Manchester', 'UK', '(161) 555-4448'),
('PB Knäckebröd AB', 'Lars Peterson', 'Sales Agent', 'Göteborg', 'Sweden', '031-987 65 43'),
('Refrescos Americanas LTDA', 'Carlos Diaz', 'Marketing Manager', 'Sao Paulo', 'Brazil', '(11) 555 4640');

-- Products
INSERT INTO products (product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES
('Chai', 1, 1, '10 boxes x 20 bags', 18.00, 39, 0, 10, FALSE),
('Chang', 1, 1, '24 - 12 oz bottles', 19.00, 17, 40, 25, FALSE),
('Aniseed Syrup', 1, 2, '12 - 550 ml bottles', 10.00, 13, 70, 25, FALSE),
('Chef Antons Cajun Seasoning', 2, 2, '48 - 6 oz jars', 22.00, 53, 0, 0, FALSE),
('Chef Antons Gumbo Mix', 2, 2, '36 boxes', 21.35, 0, 0, 0, TRUE),
('Grandmas Boysenberry Spread', 3, 2, '12 - 8 oz jars', 25.00, 120, 0, 25, FALSE),
('Uncle Bobs Organic Dried Pears', 3, 7, '12 - 1 lb pkgs.', 30.00, 15, 0, 10, FALSE),
('Northwoods Cranberry Sauce', 3, 2, '12 - 12 oz jars', 40.00, 6, 0, 0, FALSE),
('Mishi Kobe Niku', 4, 6, '18 - 500 g pkgs.', 97.00, 29, 0, 0, TRUE),
('Ikura', 4, 8, '12 - 200 ml jars', 31.00, 31, 0, 0, FALSE),
('Queso Cabrales', 5, 4, '1 kg pkg.', 21.00, 22, 30, 30, FALSE),
('Queso Manchego La Pastora', 5, 4, '10 - 500 g pkgs.', 38.00, 86, 0, 0, FALSE),
('Konbu', 6, 8, '2 kg box', 6.00, 24, 0, 5, FALSE),
('Tofu', 6, 7, '40 - 100 g pkgs.', 23.25, 35, 0, 0, FALSE),
('Genen Shouyu', 6, 2, '24 - 250 ml bottles', 15.50, 39, 0, 5, FALSE),
('Pavlova', 7, 3, '32 - 500 g boxes', 17.45, 29, 0, 10, FALSE),
('Alice Mutton', 7, 6, '20 - 1 kg tins', 39.00, 0, 0, 0, TRUE),
('Carnarvon Tigers', 7, 8, '16 kg pkg.', 62.50, 42, 0, 0, FALSE),
('Teatime Chocolate Biscuits', 8, 3, '10 boxes x 12 pieces', 9.20, 25, 0, 5, FALSE),
('Sir Rodneys Marmalade', 8, 3, '30 gift boxes', 81.00, 40, 0, 0, FALSE),
('Sir Rodneys Scones', 8, 3, '24 pkgs. x 4 pieces', 10.00, 3, 40, 5, FALSE),
('Gustaf''s Knäckebröd', 9, 5, '24 - 500 g pkgs.', 21.00, 104, 0, 25, FALSE),
('Tunnbröd', 9, 5, '12 - 250 g pkgs.', 9.00, 61, 0, 25, FALSE),
('Guaraná Fantástica', 10, 1, '12 - 355 ml cans', 4.50, 20, 0, 0, TRUE),
('NuNuCa Nuß-Nougat-Creme', 10, 3, '20 - 450 g glasses', 14.00, 76, 0, 30, FALSE);

-- Region
INSERT INTO region (region_description) VALUES
('Eastern'), ('Western'), ('Northern'), ('Southern');

-- Territories
INSERT INTO territories (territory_id, territory_description, region_id) VALUES
('01581', 'Westboro', 1),
('01730', 'Bedford', 1),
('01833', 'Georgetow', 1),
('02116', 'Boston', 1),
('02139', 'Cambridge', 1),
('02184', 'Braintree', 1),
('02903', 'Providence', 1),
('03049', 'Hollis', 3),
('03801', 'Portsmouth', 3),
('06897', 'Wilton', 1),
('07960', 'Morristown', 1),
('08837', 'Edison', 1),
('10019', 'New York', 1),
('10038', 'New York', 1),
('11747', 'Mellvile', 1),
('14450', 'Fairport', 1),
('19428', 'Philadelphia', 3),
('19713', 'Neward', 1),
('20852', 'Rockville', 1),
('27403', 'Greensboro', 1),
('27511', 'Cary', 1),
('29202', 'Columbia', 4),
('30346', 'Atlanta', 4),
('31406', 'Savannah', 4),
('32859', 'Orlando', 4),
('33607', 'Tampa', 4),
('40222', 'Louisville', 1),
('44122', 'Beachwood', 3),
('45839', 'Findlay', 3),
('48075', 'Southfield', 3),
('48084', 'Troy', 3),
('48304', 'Bloomfield Hills', 3),
('53404', 'Racine', 3),
('55113', 'Roseville', 3),
('55439', 'Minneapolis', 3),
('60179', 'Hoffman Estates', 2),
('60601', 'Chicago', 2),
('72716', 'Bentonville', 4),
('75234', 'Dallas', 4),
('78759', 'Austin', 4),
('80202', 'Denver', 2),
('80909', 'Colorado Springs', 2),
('85014', 'Phoenix', 2),
('85251', 'Scottsdale', 2),
('90405', 'Santa Monica', 2),
('94025', 'Menlo Park', 2),
('94105', 'San Francisco', 2),
('95008', 'Campbell', 2),
('95054', 'Santa Clara', 2),
('95060', 'Santa Cruz', 2),
('98004', 'Bellevue', 2),
('98052', 'Redmond', 2),
('98104', 'Seattle', 2);

-- Employees
INSERT INTO employees (last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, reports_to) VALUES
('Davolio', 'Nancy', 'Sales Representative', 'Ms.', '1948-12-08', '1992-05-01', '507 - 20th Ave. E. Apt. 2A', 'Seattle', 'WA', '98122', 'USA', '(206) 555-9857', '5467', NULL),
('Fuller', 'Andrew', 'Vice President Sales', 'Dr.', '1952-02-19', '1992-08-14', '908 W. Capital Way', 'Tacoma', 'WA', '98401', 'USA', '(206) 555-9482', '3457', NULL),
('Leverling', 'Janet', 'Sales Representative', 'Ms.', '1963-08-30', '1992-04-01', '722 Moss Bay Blvd.', 'Kirkland', 'WA', '98033', 'USA', '(206) 555-3412', '3355', 2),
('Peacock', 'Margaret', 'Sales Representative', 'Mrs.', '1937-09-19', '1993-05-03', '4110 Old Redmond Rd.', 'Redmond', 'WA', '98052', 'USA', '(206) 555-8122', '5176', 2),
('Buchanan', 'Steven', 'Sales Manager', 'Mr.', '1955-03-04', '1993-10-17', '14 Garrett Hill', 'London', NULL, 'SW1 8JR', 'UK', '(71) 555-4848', '3453', 2),
('Suyama', 'Michael', 'Sales Representative', 'Mr.', '1963-07-02', '1993-10-17', 'Coventry House Miner Rd.', 'London', NULL, 'EC2 7JR', 'UK', '(71) 555-7773', '428', 5),
('King', 'Robert', 'Sales Representative', 'Mr.', '1960-05-29', '1994-01-02', 'Edgeham Hollow Winchester Way', 'London', NULL, 'RG1 9SP', 'UK', '(71) 555-5598', '465', 5),
('Callahan', 'Laura', 'Inside Sales Coordinator', 'Ms.', '1958-01-09', '1994-03-05', '4726 - 11th Ave. N.E.', 'Seattle', 'WA', '98105', 'USA', '(206) 555-1189', '2344', 2),
('Dodsworth', 'Anne', 'Sales Representative', 'Ms.', '1966-01-27', '1994-11-15', '7 Houndstooth Rd.', 'London', NULL, 'WG2 7LT', 'UK', '(71) 555-4444', '452', 5);

-- Customers (50 sample customers)
INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES
('ALFKI', 'Alfreds Futterkiste', 'Maria Anders', 'Sales Representative', 'Obere Str. 57', 'Berlin', NULL, '12209', 'Germany', '030-0074321', '030-0076545'),
('ANATR', 'Ana Trujillo Emparedados y helados', 'Ana Trujillo', 'Owner', 'Avda. de la Constitución 2222', 'México D.F.', NULL, '05021', 'Mexico', '(5) 555-4729', '(5) 555-3745'),
('ANTON', 'Antonio Moreno Taquería', 'Antonio Moreno', 'Owner', 'Mataderos 2312', 'México D.F.', NULL, '05023', 'Mexico', '(5) 555-3932', NULL),
('AROUT', 'Around the Horn', 'Thomas Hardy', 'Sales Representative', '120 Hanover Sq.', 'London', NULL, 'WA1 1DP', 'UK', '(171) 555-7788', '(171) 555-6750'),
('BERGS', 'Berglunds snabbköp', 'Christina Berglund', 'Order Administrator', 'Berguvsvägen 8', 'Luleå', NULL, 'S-958 22', 'Sweden', '0921-12 34 65', '0921-12 34 67'),
('BLAUS', 'Blauer See Delikatessen', 'Hanna Moos', 'Sales Representative', 'Forsterstr. 57', 'Mannheim', NULL, '68306', 'Germany', '0621-08460', '0621-08924'),
('BLONP', 'Blondesddsl père et fils', 'Frédérique Citeaux', 'Marketing Manager', '24, place Kléber', 'Strasbourg', NULL, '67000', 'France', '88.60.15.31', '88.60.15.32'),
('BOLID', 'Bólido Comidas preparadas', 'Martín Sommer', 'Owner', 'C/ Araquil, 67', 'Madrid', NULL, '28023', 'Spain', '(91) 555 22 82', '(91) 555 91 99'),
('BONAP', 'Bon app''', 'Laurence Lebihan', 'Owner', '12, rue des Bouchers', 'Marseille', NULL, '13008', 'France', '91.24.45.40', '91.24.45.41'),
('BOTTM', 'Bottom-Dollar Markets', 'Elizabeth Lincoln', 'Accounting Manager', '23 Tsawassen Blvd.', 'Tsawassen', 'BC', 'T2F 8M4', 'Canada', '(604) 555-4729', '(604) 555-3745'),
('BSBEV', 'B''s Beverages', 'Victoria Ashworth', 'Sales Representative', 'Fauntleroy Circus', 'London', NULL, 'EC2 5NT', 'UK', '(171) 555-1212', NULL),
('CACTU', 'Cactus Comidas para llevar', 'Patricio Simpson', 'Sales Agent', 'Cerrito 333', 'Buenos Aires', NULL, '1010', 'Argentina', '(1) 135-5555', '(1) 135-4892'),
('CENTC', 'Centro comercial Moctezuma', 'Francisco Chang', 'Marketing Manager', 'Sierras de Granada 9993', 'México D.F.', NULL, '05022', 'Mexico', '(5) 555-3392', '(5) 555-7293'),
('CHOPS', 'Chop-suey Chinese', 'Yang Wang', 'Owner', 'Hauptstr. 29', 'Bern', NULL, '3012', 'Switzerland', '0452-076545', NULL),
('COMMI', 'Comércio Mineiro', 'Pedro Afonso', 'Sales Associate', 'Av. dos Lusíadas, 23', 'Sao Paulo', 'SP', '05432-043', 'Brazil', '(11) 555-7647', NULL),
('CONSH', 'Consolidated Holdings', 'Elizabeth Brown', 'Sales Representative', 'Berkeley Gardens 12 Brewery', 'London', NULL, 'WX1 6LT', 'UK', '(171) 555-2282', '(171) 555-9199'),
('DRACD', 'Drachenblut Delikatessen', 'Sven Ottlieb', 'Order Administrator', 'Walserweg 21', 'Aachen', NULL, '52066', 'Germany', '0241-039123', '0241-059428'),
('DUMON', 'Du monde entier', 'Janine Labrune', 'Owner', '67, rue des Cinquante Otages', 'Nantes', NULL, '44000', 'France', '40.67.88.88', '40.67.89.89'),
('EASTC', 'Eastern Connection', 'Ann Devon', 'Sales Agent', '35 King George', 'London', NULL, 'WX3 6FW', 'UK', '(171) 555-0297', '(171) 555-3373'),
('ERNSH', 'Ernst Handel', 'Roland Mendel', 'Sales Manager', 'Kirchgasse 6', 'Graz', NULL, '8010', 'Austria', '7675-3425', '7675-3426'),
('FAMIA', 'Familia Arquibaldo', 'Aria Cruz', 'Marketing Assistant', 'Rua Orós, 92', 'Sao Paulo', 'SP', '05442-030', 'Brazil', '(11) 555-9857', NULL),
('FISSA', 'FISSA Fabrica Inter. Salchichas S.A.', 'Diego Roel', 'Accounting Manager', 'C/ Moralzarzal, 86', 'Madrid', NULL, '28034', 'Spain', '(91) 555 94 44', '(91) 555 55 93'),
('FOLIG', 'Folies gourmandes', 'Martine Rancé', 'Assistant Sales Agent', '184, chaussée de Tournai', 'Lille', NULL, '59000', 'France', '20.16.10.16', '20.16.10.17'),
('FOLKO', 'Folk och fä HB', 'Maria Larsson', 'Owner', 'Åkergatan 24', 'Bräcke', NULL, 'S-844 67', 'Sweden', '0695-34 67 21', NULL),
('FRANK', 'Frankenversand', 'Peter Franken', 'Marketing Manager', 'Berliner Platz 43', 'München', NULL, '80805', 'Germany', '089-0877310', '089-0877451'),
('FRANR', 'France restauration', 'Carine Schmitt', 'Marketing Manager', '54, rue Royale', 'Nantes', NULL, '44000', 'France', '40.32.21.21', '40.32.21.20'),
('FRANS', 'Franchi S.p.A.', 'Paolo Accorti', 'Sales Representative', 'Via Monte Bianco 34', 'Torino', NULL, '10100', 'Italy', '011-4988260', '011-4988261'),
('FURIB', 'Furia Bacalhau e Frutos do Mar', 'Lino Rodriguez', 'Sales Manager', 'Jardim das rosas n. 32', 'Lisboa', NULL, '1675', 'Portugal', '(1) 354-2534', '(1) 354-2535'),
('GALED', 'Galería del gastrónomo', 'Eduardo Saavedra', 'Marketing Manager', 'Rambla de Cataluña, 23', 'Barcelona', NULL, '08022', 'Spain', '(93) 203 4560', '(93) 203 4561'),
('GODOS', 'Godos Cocina Típica', 'José Pedro Freyre', 'Sales Manager', 'C/ Romero, 33', 'Sevilla', NULL, '41101', 'Spain', '(95) 555 82 82', NULL);

-- Shippers
INSERT INTO shippers (company_name, phone) VALUES
('Speedy Express', '(503) 555-9831'),
('United Package', '(503) 555-3199'),
('Federal Shipping', '(503) 555-9931');

-- Orders (Generate orders for the last 90 days to have recent data)
-- This generates 200 sample orders
DO $$
DECLARE
    i INT;
    random_customer VARCHAR(5);
    random_employee INT;
    random_shipper INT;
    random_date DATE;
BEGIN
    FOR i IN 1..200 LOOP
        -- Random customer
        random_customer := (ARRAY['ALFKI','ANATR','ANTON','AROUT','BERGS','BLAUS','BLONP','BOLID','BONAP','BOTTM',
                                  'BSBEV','CACTU','CENTC','CHOPS','COMMI','CONSH','DRACD','DUMON','EASTC','ERNSH',
                                  'FAMIA','FISSA','FOLIG','FOLKO','FRANK','FRANR','FRANS','FURIB','GALED','GODOS'])[floor(random() * 30 + 1)];

        -- Random employee (1-9)
        random_employee := floor(random() * 9 + 1)::INT;

        -- Random shipper (1-3)
        random_shipper := floor(random() * 3 + 1)::INT;

        -- Random date in last 90 days
        random_date := CURRENT_DATE - (floor(random() * 90)::INT);

        INSERT INTO orders (customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_city, ship_country)
        VALUES (
            random_customer,
            random_employee,
            random_date,
            random_date + 7,
            CASE WHEN random() > 0.2 THEN random_date + (floor(random() * 5 + 1)::INT) ELSE NULL END,
            random_shipper,
            round((random() * 200 + 10)::numeric, 2),
            'Ship to ' || random_customer,
            (ARRAY['London','Berlin','Madrid','Paris','Lisboa','Bern','Graz','Torino','Barcelona'])[floor(random() * 9 + 1)],
            (ARRAY['UK','Germany','Spain','France','Portugal','Switzerland','Austria','Italy'])[floor(random() * 8 + 1)]
        );
    END LOOP;
END $$;

-- Order Details (2-5 items per order)
DO $$
DECLARE
    order_rec RECORD;
    num_items INT;
    i INT;
    random_product INT;
    random_qty INT;
    product_price DECIMAL(10,2);
BEGIN
    FOR order_rec IN SELECT order_id FROM orders LOOP
        -- Random number of items per order (2-5)
        num_items := floor(random() * 4 + 2)::INT;

        FOR i IN 1..num_items LOOP
            -- Random product (1-25)
            random_product := floor(random() * 25 + 1)::INT;

            -- Random quantity (1-50)
            random_qty := floor(random() * 50 + 1)::INT;

            -- Get product price
            SELECT unit_price INTO product_price FROM products WHERE product_id = random_product;

            -- Insert order detail
            INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount)
            VALUES (
                order_rec.order_id,
                random_product,
                product_price,
                random_qty,
                CASE WHEN random() > 0.7 THEN round((random() * 0.15)::numeric, 2) ELSE 0 END
            )
            ON CONFLICT DO NOTHING;  -- Skip if duplicate product in same order
        END LOOP;
    END LOOP;
END $$;

-- Create views for common queries
CREATE OR REPLACE VIEW order_subtotals AS
SELECT
    order_id,
    SUM(unit_price * quantity * (1 - discount)) AS subtotal
FROM order_details
GROUP BY order_id;

CREATE OR REPLACE VIEW product_sales_for_1997 AS
SELECT
    c.category_name,
    p.product_name,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS product_sales
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN order_details od ON p.product_id = od.product_id
JOIN orders o ON od.order_id = o.order_id
WHERE o.order_date BETWEEN '1997-01-01' AND '1997-12-31'
GROUP BY c.category_name, p.product_name;

CREATE OR REPLACE VIEW sales_by_category AS
SELECT
    c.category_id,
    c.category_name,
    p.product_name,
    SUM(od.unit_price * od.quantity * (1 - od.discount)) AS product_sales
FROM categories c
JOIN products p ON c.category_id = p.category_id
JOIN order_details od ON p.product_id = od.product_id
JOIN orders o ON od.order_id = o.order_id
GROUP BY c.category_id, c.category_name, p.product_name;

CREATE OR REPLACE VIEW quarterly_orders AS
SELECT DISTINCT
    customers.customer_id,
    customers.company_name,
    customers.city,
    customers.country
FROM customers
RIGHT JOIN orders ON customers.customer_id = orders.customer_id
WHERE orders.order_date BETWEEN '1997-01-01' AND '1997-12-31';

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO airflow;

-- Summary statistics
SELECT
    'Categories' AS table_name, COUNT(*) AS record_count FROM categories
UNION ALL
SELECT 'Suppliers', COUNT(*) FROM suppliers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Customers', COUNT(*) FROM customers
UNION ALL
SELECT 'Employees', COUNT(*) FROM employees
UNION ALL
SELECT 'Shippers', COUNT(*) FROM shippers
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Details', COUNT(*) FROM order_details
ORDER BY table_name;

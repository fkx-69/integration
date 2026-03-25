-- Run this script after creating and connecting to the target PostgreSQL database.
-- Recommended import order:
-- 1. categories
-- 2. sub_categories
-- 3. customers
-- 4. regions
-- 5. locations
-- 6. products
-- 7. orders_normalized
-- 8. order_items
-- 9. order_returns

CREATE TABLE categories (
    category_name TEXT PRIMARY KEY
);

CREATE TABLE sub_categories (
    sub_category_name TEXT PRIMARY KEY,
    category_name TEXT NOT NULL,
    CONSTRAINT fk_sub_categories_category
        FOREIGN KEY (category_name) REFERENCES categories (category_name)
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment TEXT NOT NULL
);

CREATE TABLE regions (
    region_name TEXT PRIMARY KEY,
    manager_name TEXT NULL
);

CREATE TABLE locations (
    location_id INTEGER PRIMARY KEY,
    country TEXT NOT NULL,
    state TEXT NOT NULL,
    city TEXT NOT NULL,
    postal_code TEXT NULL,
    region_name TEXT NOT NULL,
    market TEXT NOT NULL,
    CONSTRAINT fk_locations_region
        FOREIGN KEY (region_name) REFERENCES regions (region_name),
    CONSTRAINT uq_locations_natural
        UNIQUE (country, state, city, postal_code, region_name, market)
);

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    sub_category_name TEXT NOT NULL,
    CONSTRAINT fk_products_sub_category
        FOREIGN KEY (sub_category_name) REFERENCES sub_categories (sub_category_name)
);

CREATE TABLE orders_normalized (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    location_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE NOT NULL,
    ship_mode TEXT NOT NULL,
    order_priority TEXT NOT NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
    CONSTRAINT fk_orders_location
        FOREIGN KEY (location_id) REFERENCES locations (location_id)
);

CREATE TABLE order_items (
    row_id BIGINT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    sales NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    discount NUMERIC NOT NULL,
    profit NUMERIC NOT NULL,
    shipping_cost NUMERIC NOT NULL,
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders_normalized (order_id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
);

CREATE TABLE order_returns (
    order_id TEXT PRIMARY KEY,
    CONSTRAINT fk_order_returns_order
        FOREIGN KEY (order_id) REFERENCES orders_normalized (order_id)
);

CREATE INDEX idx_sub_categories_category_name
    ON sub_categories (category_name);

CREATE INDEX idx_locations_region_name
    ON locations (region_name);

CREATE INDEX idx_products_sub_category_name
    ON products (sub_category_name);

CREATE INDEX idx_orders_customer_id
    ON orders_normalized (customer_id);

CREATE INDEX idx_orders_location_id
    ON orders_normalized (location_id);

CREATE INDEX idx_order_items_order_id
    ON order_items (order_id);

CREATE INDEX idx_order_items_product_id
    ON order_items (product_id);

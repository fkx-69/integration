-- Run this script after creating and connecting to the target PostgreSQL database.
-- Recommended import order:
-- 1. categories
-- 2. sub_categories
-- 3. managers
-- 4. regions
-- 5. markets
-- 6. countries
-- 7. states
-- 8. cities
-- 9. locations
-- 10. customers
-- 11. products
-- 12. orders_normalized
-- 13. order_items
-- 14. order_returns

CREATE TABLE categories (
    category_id INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE sub_categories (
    sub_category_id INTEGER PRIMARY KEY,
    sub_category_name TEXT NOT NULL UNIQUE,
    category_id INTEGER NOT NULL,
    CONSTRAINT fk_sub_categories_category
        FOREIGN KEY (category_id) REFERENCES categories (category_id)
);

CREATE TABLE managers (
    manager_id INTEGER PRIMARY KEY,
    manager_name TEXT NOT NULL UNIQUE
);

CREATE TABLE regions (
    region_id INTEGER PRIMARY KEY,
    region_name TEXT NOT NULL UNIQUE,
    manager_id INTEGER NULL,
    CONSTRAINT fk_regions_manager
        FOREIGN KEY (manager_id) REFERENCES managers (manager_id)
);

CREATE TABLE markets (
    market_id INTEGER PRIMARY KEY,
    market_name TEXT NOT NULL UNIQUE
);

CREATE TABLE countries (
    country_id INTEGER PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE,
    market_id INTEGER NOT NULL,
    CONSTRAINT fk_countries_market
        FOREIGN KEY (market_id) REFERENCES markets (market_id)
);

CREATE TABLE states (
    state_id INTEGER PRIMARY KEY,
    state_name TEXT NOT NULL,
    country_id INTEGER NOT NULL,
    CONSTRAINT fk_states_country
        FOREIGN KEY (country_id) REFERENCES countries (country_id),
    CONSTRAINT uq_states_name_per_country
        UNIQUE (state_name, country_id)
);

CREATE TABLE cities (
    city_id INTEGER PRIMARY KEY,
    city_name TEXT NOT NULL,
    state_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    CONSTRAINT fk_cities_state
        FOREIGN KEY (state_id) REFERENCES states (state_id),
    CONSTRAINT fk_cities_region
        FOREIGN KEY (region_id) REFERENCES regions (region_id),
    CONSTRAINT uq_cities_name_per_state
        UNIQUE (city_name, state_id)
);

CREATE TABLE locations (
    location_id INTEGER PRIMARY KEY,
    city_id INTEGER NOT NULL,
    postal_code TEXT NULL,
    CONSTRAINT fk_locations_city
        FOREIGN KEY (city_id) REFERENCES cities (city_id),
    CONSTRAINT uq_locations_city_postal
        UNIQUE (city_id, postal_code)
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment TEXT NOT NULL
);

CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    sub_category_id INTEGER NOT NULL,
    CONSTRAINT fk_products_sub_category
        FOREIGN KEY (sub_category_id) REFERENCES sub_categories (sub_category_id)
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

CREATE INDEX idx_sub_categories_category_id
    ON sub_categories (category_id);

CREATE INDEX idx_regions_manager_id
    ON regions (manager_id);

CREATE INDEX idx_countries_market_id
    ON countries (market_id);

CREATE INDEX idx_states_country_id
    ON states (country_id);

CREATE INDEX idx_cities_state_id
    ON cities (state_id);

CREATE INDEX idx_cities_region_id
    ON cities (region_id);

CREATE INDEX idx_locations_city_id
    ON locations (city_id);

CREATE INDEX idx_products_sub_category_id
    ON products (sub_category_id);

CREATE INDEX idx_orders_customer_id
    ON orders_normalized (customer_id);

CREATE INDEX idx_orders_location_id
    ON orders_normalized (location_id);

CREATE INDEX idx_order_items_order_id
    ON order_items (order_id);

CREATE INDEX idx_order_items_product_id
    ON order_items (product_id);

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Use analytics database
USE analytics;

-- Products table
CREATE TABLE IF NOT EXISTS products (
    shop_id String,
    product_id String,
    title String,
    vendor String,
    product_type String,
    created_at DateTime64(3) 'UTC',
    updated_at DateTime64(3) 'UTC',
    variants_count UInt32,
    min_price Float64,
    max_price Float64,
    currency String,
    tags String,
    status String,
    type String,
    event_timestamp DateTime64(3) 'UTC'
) ENGINE = MergeTree()
ORDER BY (shop_id, product_id, updated_at)
PARTITION BY toYYYYMM(updated_at);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    shop_id String,
    order_id String,
    order_number String,
    customer_id String,
    email String,
    total_price Float64,
    currency String,
    financial_status String,
    fulfillment_status String,
    created_at DateTime64(3) 'UTC',
    updated_at DateTime64(3) 'UTC',
    item_count UInt32,
    total_discounts Float64,
    total_tax Float64,
    type String,
    event_timestamp DateTime64(3) 'UTC'
) ENGINE = MergeTree()
ORDER BY (shop_id, order_id, updated_at)
PARTITION BY toYYYYMM(updated_at);

-- Subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    shop_id String,
    subscription_id String,
    customer_id String,
    status String,
    created_at DateTime64(3) 'UTC',
    updated_at DateTime64(3) 'UTC',
    next_billing_date Date,
    interval String,
    interval_count UInt32,
    product_id String,
    variant_id String,
    quantity UInt32,
    price Float64,
    currency String,
    type String,
    event_timestamp DateTime64(3) 'UTC'
) ENGINE = MergeTree()
ORDER BY (shop_id, subscription_id, updated_at)
PARTITION BY toYYYYMM(updated_at);

-- Raw events table (for any other event types)
CREATE TABLE IF NOT EXISTS events_raw (
    shop_id String,
    event_data String,
    event_type String,
    event_timestamp DateTime64(3) 'UTC'
) ENGINE = MergeTree()
ORDER BY (shop_id, event_type, event_timestamp)
PARTITION BY toYYYYMM(event_timestamp);

-- Create materialized views for analytics

-- Daily product stats by shop
CREATE MATERIALIZED VIEW IF NOT EXISTS product_daily_stats
ENGINE = SummingMergeTree()
ORDER BY (shop_id, date, product_type)
POPULATE
AS SELECT
    shop_id,
    toDate(updated_at) AS date,
    product_type,
    count() AS total_products,
    uniqExact(product_id) AS unique_products,
    avg(min_price) AS avg_min_price,
    avg(max_price) AS avg_max_price
FROM products
GROUP BY shop_id, date, product_type;

-- Daily order stats by shop
CREATE MATERIALIZED VIEW IF NOT EXISTS order_daily_stats
ENGINE = SummingMergeTree()
ORDER BY (shop_id, date, financial_status)
POPULATE
AS SELECT
    shop_id,
    toDate(created_at) AS date,
    financial_status,
    count() AS total_orders,
    sum(total_price) AS total_revenue,
    avg(total_price) AS avg_order_value,
    sum(item_count) AS total_items_sold
FROM orders
GROUP BY shop_id, date, financial_status;

-- Daily subscription stats by shop
CREATE MATERIALIZED VIEW IF NOT EXISTS subscription_daily_stats
ENGINE = SummingMergeTree()
ORDER BY (shop_id, date, status)
POPULATE
AS SELECT
    shop_id,
    toDate(created_at) AS date,
    status,
    count() AS total_subscriptions,
    uniqExact(customer_id) AS unique_customers,
    sum(price * quantity) AS total_recurring_revenue
FROM subscriptions
GROUP BY shop_id, date, status;

-- Create user-specific views for multi-tenancy

-- Function to create shop-specific views
CREATE FUNCTION IF NOT EXISTS create_shop_views AS (shop_id String) ->
(
    -- Create shop-specific product view
    CREATE OR REPLACE VIEW shop_{shop_id}_products AS
    SELECT * FROM products WHERE shop_id = {shop_id};
    
    -- Create shop-specific orders view
    CREATE OR REPLACE VIEW shop_{shop_id}_orders AS
    SELECT * FROM orders WHERE shop_id = {shop_id};
    
    -- Create shop-specific subscriptions view
    CREATE OR REPLACE VIEW shop_{shop_id}_subscriptions AS
    SELECT * FROM subscriptions WHERE shop_id = {shop_id};
    
    -- Create shop-specific stats views
    CREATE OR REPLACE VIEW shop_{shop_id}_product_stats AS
    SELECT * FROM product_daily_stats WHERE shop_id = {shop_id};
    
    CREATE OR REPLACE VIEW shop_{shop_id}_order_stats AS
    SELECT * FROM order_daily_stats WHERE shop_id = {shop_id};
    
    CREATE OR REPLACE VIEW shop_{shop_id}_subscription_stats AS
    SELECT * FROM subscription_daily_stats WHERE shop_id = {shop_id};
);

-- Example of creating views for a specific shop (can be called dynamically)
-- SELECT create_shop_views('shop123'); 
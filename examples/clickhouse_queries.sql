-- Basic multi-tenant queries for Shopify analytics

-- Switch to analytics database
USE analytics;

-- =============================================
-- Basic Shop-Specific Queries
-- =============================================

-- Get all products for a specific shop
SELECT * 
FROM products 
WHERE shop_id = 'shop_1001'
ORDER BY updated_at DESC
LIMIT 10;

-- Get all orders for a specific shop
SELECT * 
FROM orders 
WHERE shop_id = 'shop_1001'
ORDER BY updated_at DESC
LIMIT 10;

-- Get all subscriptions for a specific shop
SELECT * 
FROM subscriptions 
WHERE shop_id = 'shop_1001'
ORDER BY updated_at DESC
LIMIT 10;

-- =============================================
-- Create Shop-Specific Views
-- =============================================

-- Create views for a specific shop
SELECT create_shop_views('shop_1001');

-- Query shop-specific views
SELECT * FROM shop_shop_1001_products LIMIT 10;
SELECT * FROM shop_shop_1001_orders LIMIT 10;
SELECT * FROM shop_shop_1001_subscriptions LIMIT 10;

-- =============================================
-- Shop Analytics Queries
-- =============================================

-- Revenue by day for a specific shop
SELECT 
    toDate(created_at) AS date,
    sum(total_price) AS daily_revenue
FROM orders
WHERE shop_id = 'shop_1001'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- Product popularity by type for a specific shop
SELECT 
    product_type,
    count() AS product_count,
    uniqExact(product_id) AS unique_products
FROM products
WHERE shop_id = 'shop_1001'
GROUP BY product_type
ORDER BY product_count DESC;

-- Subscription status distribution for a specific shop
SELECT 
    status,
    count() AS subscription_count,
    sum(price * quantity) AS recurring_revenue
FROM subscriptions
WHERE shop_id = 'shop_1001'
GROUP BY status
ORDER BY subscription_count DESC;

-- =============================================
-- Multi-Shop Comparative Analytics
-- =============================================

-- Compare revenue across shops
SELECT 
    shop_id,
    toDate(created_at) AS date,
    sum(total_price) AS daily_revenue
FROM orders
WHERE toDate(created_at) >= today() - 30
GROUP BY shop_id, date
ORDER BY shop_id, date DESC;

-- Compare product counts across shops
SELECT 
    shop_id,
    product_type,
    count() AS product_count
FROM products
GROUP BY shop_id, product_type
ORDER BY shop_id, product_count DESC;

-- Compare subscription metrics across shops
SELECT 
    shop_id,
    status,
    count() AS subscription_count,
    sum(price * quantity) AS recurring_revenue
FROM subscriptions
GROUP BY shop_id, status
ORDER BY shop_id, recurring_revenue DESC;

-- =============================================
-- Advanced Multi-Tenant Analytics
-- =============================================

-- Top performing shops by revenue
SELECT 
    shop_id,
    sum(total_price) AS total_revenue,
    count() AS order_count,
    avg(total_price) AS avg_order_value
FROM orders
WHERE toDate(created_at) >= today() - 30
GROUP BY shop_id
ORDER BY total_revenue DESC;

-- Shop growth rate (comparing current month to previous month)
WITH 
    current_month AS (
        SELECT 
            shop_id,
            sum(total_price) AS revenue
        FROM orders
        WHERE toStartOfMonth(created_at) = toStartOfMonth(now())
        GROUP BY shop_id
    ),
    previous_month AS (
        SELECT 
            shop_id,
            sum(total_price) AS revenue
        FROM orders
        WHERE toStartOfMonth(created_at) = addMonths(toStartOfMonth(now()), -1)
        GROUP BY shop_id
    )
SELECT 
    c.shop_id,
    c.revenue AS current_revenue,
    p.revenue AS previous_revenue,
    (c.revenue - p.revenue) / p.revenue * 100 AS growth_percentage
FROM current_month c
JOIN previous_month p ON c.shop_id = p.shop_id
ORDER BY growth_percentage DESC;

-- Product category performance across all shops
SELECT 
    product_type,
    count() AS total_products,
    uniqExact(shop_id) AS shop_count,
    avg(min_price) AS avg_min_price,
    avg(max_price) AS avg_max_price
FROM products
GROUP BY product_type
ORDER BY total_products DESC;

-- =============================================
-- Time-Series Analysis
-- =============================================

-- Daily revenue trend for a specific shop
SELECT 
    toDate(created_at) AS date,
    sum(total_price) AS daily_revenue
FROM orders
WHERE shop_id = 'shop_1001'
  AND toDate(created_at) >= today() - 30
GROUP BY date
ORDER BY date;

-- Weekly subscription growth for a specific shop
SELECT 
    toStartOfWeek(created_at) AS week,
    count() AS new_subscriptions
FROM subscriptions
WHERE shop_id = 'shop_1001'
  AND type = 'subscription_created'
GROUP BY week
ORDER BY week;

-- =============================================
-- Data Export for Business Intelligence
-- =============================================

-- Export shop data for external analysis
SELECT 
    shop_id,
    toDate(created_at) AS date,
    count() AS order_count,
    sum(total_price) AS total_revenue,
    avg(total_price) AS avg_order_value,
    sum(total_discounts) AS total_discounts,
    sum(item_count) AS items_sold
FROM orders
WHERE toDate(created_at) >= today() - 90
GROUP BY shop_id, date
ORDER BY shop_id, date; 
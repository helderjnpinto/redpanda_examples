CREATE OR REPLACE VIEW orders_events AS
SELECT
    'free-soul-sistas' AS tenant_id,
    o.created_at AS event_time,
    'order_created' AS event_type,
    NULL AS status_after, --TODO: 'active' ?
    so.subscription_id AS subscription_id,
    o.customer_id AS customer_id,
    CASE
        WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
        WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
        ELSE 'other' --TODO: aceitar 'onetime' e 'addon'
        END AS revenue_type,
    o.total_price * 100 AS price_cents, --TODO: confirmar
    o.total_tax * 100 AS tax_cents,
    s.delivery_price_amount * 100 AS shipping_cents, --TODO: confirmar
    NULL AS interval_days, --TODO: vem da subscriptions
    NULL AS plan_id,
    NULL AS region,
    o.source AS channel,
    o.currency AS currency,
    NULL AS cancellation_reason,
    NULL AS pause_reason
FROM orders o
    LEFT JOIN subscription_orders so
        ON o.id = so.order_id
    LEFT JOIN subscriptions s
        ON so.subscription_id = s.id
WHERE o.state != 'cancelled';


-- select revenue_type, count(*) from orders_events
-- group by revenue_type ;
--
-- select tags, count(*) from orders where state != 'cancelled'
-- group by tags;
--
-- SELECT
--     CASE
--         WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
--         WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
--         ELSE 'other'
--         END AS revenue_type,
--     count(*)
-- FROM orders o
-- where o.state != 'cancelled'
-- group by revenue_type;

-- select *, unnest(tags) as tag from orders
-- where id = '0001b2c5-beab-4269-a138-6b2f85791e02';
--
-- SELECT o.*, t AS tag
-- FROM orders o
--          CROSS JOIN LATERAL unnest(o.tags) AS t
-- WHERE t IN ('Subscription Recurring Order', 'Subscription First Order') AND o.id = '00003a97-b358-4e6f-96c4-338359ec2335';

CREATE OR REPLACE VIEW subscription_events AS
SELECT
    'free-soul-sistas' AS tenant_id,
    s.created_at AS event_time,
    'sub_created' AS event_type,
    'active' AS status_after,
    s.id AS subscription_id,
    s.customer_id AS customer_id,
    CASE
        WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
        WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
        ELSE 'other'
        END AS revenue_type,
    o.total_price * 100 AS price_cents,
    o.total_tax * 100 AS tax_cents,
    s.delivery_price_amount * 100 AS shipping_cents,
    NULL AS interval_days,
    NULL AS plan_id,
    NULL AS region,
    s.source AS channel,
    s.currency_code AS currency,
    NULL AS cancellation_reason,
    NULL AS pause_reason
FROM subscriptions s
    JOIN orders o on s.origin_order_id = o.id

UNION ALL

SELECT
    'free-soul-sistas' AS tenant_id,
    st.created_at AS event_time,
    'sub_paused' AS event_type,
    'paused' AS status_after,
    st.subscription_id AS subscription_id,
    s.customer_id AS customer_id,
    CASE
        WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
        WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
        ELSE 'other'
        END AS revenue_type,
    o.total_price * 100 AS price_cents,
    o.total_tax * 100 AS tax_cents,
    s.delivery_price_amount * 100 AS shipping_cents,
    NULL AS interval_days,
    NULL AS plan_id,
    NULL AS region,
    s.source AS channel,
    s.currency_code AS currency,
    NULL AS cancellation_reason,
    NULL AS pause_reason
FROM subscription_transitions st
    JOIN subscriptions s ON st.subscription_id = s.id
    JOIN orders o on s.origin_order_id = o.id
WHERE to_state IN ('paused', 'skipped')

UNION ALL

SELECT
    'free-soul-sistas' AS tenant_id,
    st.created_at AS event_time,
    'sub_cancelled' AS event_type,
    'cancelled' AS status_after,
    st.subscription_id AS subscription_id,
    s.customer_id AS customer_id,
    CASE
        WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
        WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
        ELSE 'other'
        END AS revenue_type,
    o.total_price * 100 AS price_cents,
    o.total_tax * 100 AS tax_cents,
    s.delivery_price_amount * 100 AS shipping_cents,
    NULL AS interval_days,
    NULL AS plan_id,
    NULL AS region,
    s.source AS channel,
    s.currency_code AS currency,
    s.cancellation_reason AS cancellation_reason,
    NULL AS pause_reason
FROM subscription_transitions st
    JOIN subscriptions s ON st.subscription_id = s.id
    JOIN orders o on s.origin_order_id = o.id
WHERE to_state IN ('cancelled', 'failed')

UNION ALL

SELECT
    'free-soul-sistas' AS tenant_id,
    curr_st.created_at AS event_time,
    CASE prev_st.to_state
        WHEN 'paused' THEN 'sub_resumed'
        WHEN 'cancelled' THEN 'sub_reactivated'
        END AS event_type,
    'active' AS status_after,
    curr_st.subscription_id AS subscription_id,
    s.customer_id AS customer_id,
    CASE
        WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
        WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
        ELSE 'other'
        END AS revenue_type,
    o.total_price * 100 AS price_cents,
    o.total_tax * 100 AS tax_cents,
    s.delivery_price_amount * 100 AS shipping_cents,
    NULL AS interval_days,
    NULL AS plan_id,
    NULL AS region,
    s.source AS channel,
    s.currency_code AS currency,
    NULL AS cancellation_reason,
    NULL AS pause_reason
FROM subscription_transitions curr_st
    JOIN subscription_transitions prev_st
        ON prev_st.subscription_id = curr_st.subscription_id
               AND prev_st.sort_key = curr_st.sort_key - 10
    JOIN subscriptions s ON curr_st.subscription_id = s.id
    JOIN orders o on s.origin_order_id = o.id
WHERE curr_st.to_state = 'active'
  AND curr_st.sort_key != 10
  AND prev_st.to_state IN ('paused', 'cancelled');

-- select event_type, count(*)from subscription_events
-- group by event_type;

-- select * from subscriptions;

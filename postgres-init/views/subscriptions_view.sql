CREATE OR REPLACE VIEW subscription_events AS
SELECT 'free-soul-sistas'                         AS tenant_id,
       s.created_at                               AS event_time,
       'sub_created'                              AS event_type,
       'active'                                   AS status_after,
       s.id                                       AS subscription_id,
       s.customer_id                              AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           END                                    AS revenue_type,
       coalesce(o.subtotal_price * 100, 0)        AS price_cents,
       coalesce(o.total_tax * 100, 0)             AS tax_cents,
       coalesce(s.delivery_price_amount * 100, 0) AS shipping_cents,
       30                                         AS interval_days,
       NULL                                       AS plan_id,
       NULL                                       AS region,
       s.source                                   AS channel,
       s.currency_code                            AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM subscriptions s
         JOIN orders o on s.origin_order_id = o.id
WHERE o.state NOT IN ('cancelled', 'refunded')

UNION ALL

SELECT 'free-soul-sistas'                         AS tenant_id,
       st.created_at                              AS event_time,
       'sub_paused'                               AS event_type,
       'paused'                                   AS status_after,
       st.subscription_id                         AS subscription_id,
       s.customer_id                              AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           END                                    AS revenue_type,
       coalesce(o.subtotal_price * 100, 0)        AS price_cents,
       coalesce(o.total_tax * 100, 0)             AS tax_cents,
       coalesce(s.delivery_price_amount * 100, 0) AS shipping_cents,
       30                                         AS interval_days,
       NULL                                       AS plan_id,
       NULL                                       AS region,
       s.source                                   AS channel,
       s.currency_code                            AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM subscription_transitions st
         JOIN subscriptions s ON st.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE to_state IN ('paused', 'skipped')
  AND o.state NOT IN ('cancelled', 'refunded')

UNION ALL

SELECT 'free-soul-sistas'                         AS tenant_id,
       st.created_at                              AS event_time,
       'sub_cancelled'                            AS event_type,
       'cancelled'                                AS status_after,
       st.subscription_id                         AS subscription_id,
       s.customer_id                              AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           END                                    AS revenue_type,
       coalesce(o.subtotal_price * 100, 0)        AS price_cents,
       coalesce(o.total_tax * 100, 0)             AS tax_cents,
       coalesce(s.delivery_price_amount * 100, 0) AS shipping_cents,
       30                                         AS interval_days,
       NULL                                       AS plan_id,
       NULL                                       AS region,
       s.source                                   AS channel,
       s.currency_code                            AS currency,
       coalesce(s.cancellation_reason, 'Other')   AS cancellation_reason,
       NULL                                       AS pause_reason
FROM subscription_transitions st
         JOIN subscriptions s ON st.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE to_state IN ('cancelled', 'failed')
  AND o.state NOT IN ('cancelled', 'refunded')

UNION ALL

SELECT 'free-soul-sistas'                         AS tenant_id,
       curr_st.created_at                         AS event_time,
       CASE prev_st.to_state
           WHEN 'paused' THEN 'sub_resumed'
           WHEN 'cancelled' THEN 'sub_reactivated'
           END                                    AS event_type,
       'active'                                   AS status_after,
       curr_st.subscription_id                    AS subscription_id,
       s.customer_id                              AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           END                                    AS revenue_type,
       coalesce(o.subtotal_price * 100, 0)        AS price_cents,
       coalesce(o.total_tax * 100, 0)             AS tax_cents,
       coalesce(s.delivery_price_amount * 100, 0) AS shipping_cents,
       30                                         AS interval_days,
       NULL                                       AS plan_id,
       NULL                                       AS region,
       s.source                                   AS channel,
       s.currency_code                            AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM subscription_transitions curr_st
         JOIN subscription_transitions prev_st
              ON prev_st.subscription_id = curr_st.subscription_id
                  AND prev_st.sort_key = curr_st.sort_key - 10
         JOIN subscriptions s ON curr_st.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE curr_st.to_state = 'active'
  AND curr_st.sort_key != 10
  AND prev_st.to_state IN ('paused', 'cancelled')
  AND o.state NOT IN ('cancelled', 'refunded');

select event_type, count(*)from subscription_events
group by event_type;

-- select * from subscriptions;

select subscription_events.revenue_type, count(*)
from subscription_events
where event_type = 'sub_paused'
group by subscription_events.revenue_type;
CREATE OR REPLACE VIEW dunning_events AS
SELECT 'free-soul-sistas'                         AS tenant_id,
       d.created_at                               AS event_time,
       'dunning_entered'                          AS event_type,
       'dunning'                                  AS status_after,
       d.subscription_id                          AS subscription_id,
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
       o.source                                   AS channel,
       o.currency                                 AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM dunning_counters d
         JOIN subscriptions s on d.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE d.type = 'FailedBillingCounter'
  AND d.state = 'closed'
  AND o.state NOT IN ('cancelled', 'refunded')

UNION ALL

SELECT 'free-soul-sistas'                                                                          AS tenant_id,
       d.updated_at                                                                                AS event_time,
       'dunning_exited'                                                                            AS event_type,
       'cancelled'                                                                                 AS status_after,
       d.subscription_id                                                                           AS subscription_id,
       s.customer_id                                                                               AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           END                                                                                     AS revenue_type,
       coalesce(o.subtotal_price * 100, 0)                                                         AS price_cents,
       coalesce(o.total_tax * 100, 0)                                                              AS tax_cents,
       coalesce(s.delivery_price_amount * 100, 0)                                                  AS shipping_cents,
       30                                                                                          AS interval_days,
       NULL                                                                                        AS plan_id,
       NULL                                                                                        AS region,
       o.source                                                                                    AS channel,
       o.currency                                                                                  AS currency,
       coalesce(s.cancellation_reason,
                'Failed due to exceeding maximum allowed failed billings.')                        AS cancellation_reason,
       NULL                                                                                        AS pause_reason
FROM dunning_counters d
         JOIN subscriptions s on d.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE d.type = 'FailedBillingCounter'
  AND d.state = 'closed'
  AND failed_cycles = max_failed_cycles
  AND o.state NOT IN ('cancelled', 'refunded')

UNION ALL

SELECT 'free-soul-sistas'                         AS tenant_id,
       d.updated_at                               AS event_time,
       'dunning_recovered'                        AS event_type,
       'active'                                   AS status_after,
       d.subscription_id                          AS subscription_id,
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
       o.source                                   AS channel,
       o.currency                                 AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM dunning_counters d
         JOIN subscriptions s on d.subscription_id = s.id
         JOIN orders o on s.origin_order_id = o.id
WHERE d.type = 'FailedBillingCounter'
  AND d.state = 'closed'
  AND failed_cycles < max_failed_cycles
  AND o.state NOT IN ('cancelled', 'refunded');

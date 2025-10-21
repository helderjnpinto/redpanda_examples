CREATE OR REPLACE VIEW dunning_events AS
SELECT
     'free-soul-sistas' AS tenant_id,
     created_at AS event_time,
     'dunning_entered' AS event_type,
     NULL AS status_after,
     NULL AS subscription_id,
     NULL AS customer_id,
     NULL AS revenue_type,
     NULL AS price_cents,
     NULL AS interval_days,
     NULL AS plan_id,
     NULL AS region,
     NULL AS channel,
     NULL AS currency,
     NULL AS cancellation_reason,
     NULL AS pause_reason
FROM dunning_counters
WHERE type = 'FailedBillingCounter'
    AND state = 'closed'

UNION ALL

SELECT
     'free-soul-sistas' AS tenant_id,
     updated_at AS event_time,
     'dunning_exited' AS event_type,
     NULL AS status_after,
     NULL AS subscription_id,
     NULL AS customer_id,
     NULL AS revenue_type,
     NULL AS price_cents,
     NULL AS interval_days,
     NULL AS plan_id,
     NULL AS region,
     NULL AS channel,
     NULL AS currency,
     NULL AS cancellation_reason,
     NULL AS pause_reason
FROM dunning_counters
WHERE type = 'FailedBillingCounter'
    AND state = 'closed'
    AND failed_cycles = max_failed_cycles

UNION ALL

SELECT
     'free-soul-sistas' AS tenant_id,
     updated_at AS event_time,
     'dunning_recovered' AS event_type,
     NULL AS status_after,
     NULL AS subscription_id,
     NULL AS customer_id,
     NULL AS revenue_type,
     NULL AS price_cents,
     NULL AS interval_days,
     NULL AS plan_id,
     NULL AS region,
     NULL AS channel,
     NULL AS currency,
     NULL AS cancellation_reason,
     NULL AS pause_reason
FROM dunning_counters
WHERE type = 'FailedBillingCounter'
    AND state = 'closed'
    AND failed_cycles < max_failed_cycles;

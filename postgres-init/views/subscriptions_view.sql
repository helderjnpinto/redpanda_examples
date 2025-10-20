-- Subscriptions Created
SELECT
    'free-soul-sistas' AS tenant_id,
    created_at AS event_time,
    'sub_created' AS event_type
FROM subscriptions;

-- Subscriptions Paused
SELECT
    'free-soul-sistas' AS tenant_id,
    created_at AS event_time,
    'sub_paused' AS event_type
FROM subscription_transitions
WHERE to_state in ('paused', 'skipped');

-- Subscriptions Cancelled
SELECT
    'free-soul-sistas' AS tenant_id,
    created_at AS event_time,
    'sub_cancelled' AS event_type
FROM subscription_transitions
WHERE to_state in ('cancelled', 'failed');

--TODO: sub_resumed, sub_reactivated
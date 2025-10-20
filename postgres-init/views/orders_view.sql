CREATE MATERIALIZED VIEW mv_order_events AS
SELECT
    row_number() over () as row_id,
    'free-soul-sistas' as tenant_id,
    created_at AS event_time,
    'order_created' AS event_type
FROM orders
WHERE state != 'cancelled';

CREATE UNIQUE INDEX idx_mv_order_events_row_id ON mv_order_events (row_id);

REFRESH MATERIALIZED VIEW CONCURRENTLY mv_order_events;

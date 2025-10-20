CREATE MATERIALIZED VIEW mv_dunning_events AS
SELECT
            row_number() OVER () AS row_id,
            tenant_id,
            event_time,
            event_type
FROM (
         -- Entered Dunning
         SELECT
             'free-soul-sistas' AS tenant_id,
             created_at AS event_time,
             'dunning_entered' AS event_type
         FROM dunning_counters
         WHERE type = 'FailedBillingCounter'
           AND state = 'closed'

         UNION ALL

         -- Exited Dunning
         SELECT
             'free-soul-sistas' AS tenant_id,
             updated_at AS event_time,
             'dunning_exited' AS event_type
         FROM dunning_counters
         WHERE type = 'FailedBillingCounter'
           AND state = 'closed'
           AND failed_cycles = max_failed_cycles

         UNION ALL

         -- Recovered Dunning
         SELECT
             'free-soul-sistas' AS tenant_id,
             updated_at AS event_time,
             'dunning_recovered' AS event_type
         FROM dunning_counters
         WHERE type = 'FailedBillingCounter'
           AND state = 'closed'
           AND failed_cycles < max_failed_cycles
     ) AS combined;

CREATE UNIQUE INDEX idx_mv_dunning_events_row_id
    ON mv_dunning_events (row_id);

REFRESH MATERIALIZED VIEW CONCURRENTLY mv_dunning_events;

select event_type, count(*) from mv_dunning_events group by event_type;
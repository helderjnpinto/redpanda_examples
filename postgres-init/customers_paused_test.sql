SELECT *
FROM (
         SELECT md5(
                        concat_ws(
                                '::',
                                'free-soul-sistas',
                                s_last.paused_at::text,
                                'customer_paused',
                                c.id::text
                        )
                )                                                    AS event_id,
                'free-soul-sistas'                                   AS tenant_id,
                s_last.paused_at                                    AS event_time,
                'customer_paused'                                    AS event_type,
                'paused'                                             AS status_after,
                s_last.id                                            AS subscription_id,
                c.id                                                 AS customer_id,
                'recurring'                                          AS revenue_type,
                0                                                    AS price_cents,
                0                                                    AS tax_cents,
                0                                                    AS shipping_cents,
                0                                                    AS interval_days,
                NULL                                                 AS plan_id,
                NULL                                                 AS region,
                NULL                                                 AS channel,
                NULL                                                 AS currency,
                NULL                                                 AS cancellation_reason,
                NULL                                                 AS pause_reason
         FROM customers c
                  JOIN (
             SELECT s.customer_id,
                    MAX(s.paused_at) AS last_paused_at
             FROM subscriptions s
             GROUP BY s.customer_id
             HAVING COUNT(*) > 0
                AND BOOL_AND(s.state = 'paused')
         ) all_paused ON all_paused.customer_id = c.id
                  JOIN LATERAL (
             SELECT s.*
             FROM subscriptions s
             WHERE s.customer_id = c.id
               AND s.state = 'paused'
               AND s.paused_at = all_paused.last_paused_at
             ORDER BY s.id DESC
             LIMIT 1
             ) s_last ON TRUE
     ) AS query
WHERE event_time >= '2025-09-01' AND event_time < '2025-10-01'
ORDER BY event_time DESC

select state from subscriptions group by state;
SELECT *
FROM (
         SELECT md5(
                        concat_ws(
                                '::',
                                'free-soul-sistas',
                                s_last.cancellation_date::text,
                                'customer_cancelled',
                                c.id::text
                        )
                )                                                    AS event_id,
                'free-soul-sistas'                                   AS tenant_id,
                s_last.cancellation_date                             AS event_time,
                'customer_cancelled'                                 AS event_type,
                'active'                                             AS status_after,
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
                s_last.cancellation_reason                           AS cancellation_reason,
                NULL                                                 AS pause_reason
         FROM customers c
                  JOIN (
                           SELECT s.customer_id,
                                  MAX(s.cancellation_date) AS last_cancelled_at
                           FROM subscriptions s
                           GROUP BY s.customer_id
                           HAVING COUNT(*) > 0
                              AND BOOL_AND(s.state = 'cancelled')
                       ) all_cancelled ON all_cancelled.customer_id = c.id
                  JOIN LATERAL (
                           SELECT s.*
                           FROM subscriptions s
                           WHERE s.customer_id = c.id
                             AND s.state = 'cancelled'
                             AND s.cancellation_date = all_cancelled.last_cancelled_at
                           ORDER BY s.id DESC
                           LIMIT 1
                       ) s_last ON TRUE
     ) AS query
WHERE event_time >= '2025-09-01' AND event_time < '2025-10-01'
ORDER BY event_time DESC;

select c.id, s.id, s.cancellation_date, s.created_at, s.state from customers c join subscriptions s on c.id = s.customer_id
         where c.id = '74c61023-1581-4d26-af25-70cf36d7d4d2';
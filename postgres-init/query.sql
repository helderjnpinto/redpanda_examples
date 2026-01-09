SELECT *
FROM (SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             o.created_at::text,
                             'order_created',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 WHEN so.subscription_id IS NULL THEN 'onetime'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             o.created_at                                         AS event_time,
             'order_created'                                      AS event_type,
             'active'                                             AS status_after,
             so.subscription_id                                   AS subscription_id,
             o.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 WHEN so.subscription_id is NULL THEN 'onetime'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents, --- subtrair a tax
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                                 AS plan_id,
             NULL                                                 AS region,
             o.source                                             AS channel,
             o.currency                                           AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM orders o
               LEFT JOIN subscription_orders so
                         ON o.id = so.order_id
               LEFT JOIN subscriptions s
                         ON so.subscription_id = s.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             s.created_at::text,
                             'sub_created',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             s.created_at                                         AS event_time,
             'sub_created'                                        AS event_type,
             'active'                                             AS status_after,
             s.id                                                 AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                                 AS plan_id,
             NULL                                                 AS region,
             s.source                                             AS channel,
             s.currency_code                                      AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM subscriptions s
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             st.created_at::text,
                             'sub_paused',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             st.created_at                                        AS event_time,
             'sub_paused'                                         AS event_type,
             'paused'                                             AS status_after,
             st.subscription_id                                   AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                               AS plan_id,
             NULL                                                 AS region,
             s.source                                             AS channel,
             s.currency_code                                      AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM subscription_transitions st
               JOIN subscriptions s ON st.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE to_state IN ('paused', 'skipped')
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             st.created_at::text,
                             'sub_cancelled',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             st.created_at                                        AS event_time,
             'sub_cancelled'                                      AS event_type,
             'cancelled'                                          AS status_after,
             st.subscription_id                                   AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                               AS plan_id,
             NULL                                                 AS region,
             s.source                                             AS channel,
             s.currency_code                                      AS currency,
             coalesce(s.cancellation_reason, 'Other')             AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM subscription_transitions st
               JOIN subscriptions s ON st.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE to_state IN ('cancelled', 'failed')
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             curr_st.created_at::text,
                             CASE prev_st.to_state
                                 WHEN 'paused' THEN 'sub_resumed'
                                 WHEN 'cancelled' THEN 'sub_reactivated'
                                 END,
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             curr_st.created_at                                   AS event_time,
             CASE prev_st.to_state
                 WHEN 'paused' THEN 'sub_resumed'
                 WHEN 'cancelled' THEN 'sub_reactivated'
                 END                                              AS event_type,
             'active'                                             AS status_after,
             curr_st.subscription_id                              AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                              AS plan_id,
             NULL                                                 AS region,
             s.source                                             AS channel,
             s.currency_code                                      AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM subscription_transitions curr_st
               JOIN subscription_transitions prev_st
                    ON prev_st.subscription_id = curr_st.subscription_id
                        AND prev_st.sort_key = curr_st.sort_key - 10
               JOIN subscriptions s ON curr_st.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE curr_st.to_state = 'active'
        AND curr_st.sort_key != 10
        AND prev_st.to_state IN ('paused', 'cancelled')
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             d.created_at::text,
                             'dunning_entered',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             d.created_at                                         AS event_time,
             'dunning_entered'                                    AS event_type,
             'dunning'                                            AS status_after,
             d.subscription_id                                    AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                             AS plan_id,
             NULL                                                 AS region,
             o.source                                             AS channel,
             o.currency                                           AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM dunning_counters d
               JOIN subscriptions s on d.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE d.type = 'FailedBillingCounter'
        AND d.state = 'closed'
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             d.updated_at::text,
                             'dunning_exited',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                                    AS event_id,
             'free-soul-sistas'                                                   AS tenant_id,
             d.updated_at                                                         AS event_time,
             'dunning_exited'                                                     AS event_type,
             'cancelled'                                                          AS status_after,
             d.subscription_id                                                    AS subscription_id,
             s.customer_id                                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)                        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)                             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0)                 AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                                              AS interval_days,
             sp.name                                                               AS plan_id,
             NULL                                                                 AS region,
             o.source                                                             AS channel,
             o.currency                                                           AS currency,
             coalesce(s.cancellation_reason,
                      'Failed due to exceeding maximum allowed failed billings.') AS cancellation_reason,
             NULL                                                                 AS pause_reason
      FROM dunning_counters d
               JOIN subscriptions s on d.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE d.type = 'FailedBillingCounter'
        AND d.state = 'closed'
        AND failed_cycles = max_failed_cycles
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             d.updated_at::text,
                             'dunning_recovered',
                             CASE
                                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                                 END,
                             s.id::text,
                             o.id::text
                     )
             )                                                    AS event_id,
             'free-soul-sistas'                                   AS tenant_id,
             d.updated_at                                         AS event_time,
             'dunning_recovered'                                  AS event_type,
             'active'                                             AS status_after,
             d.subscription_id                                    AS subscription_id,
             s.customer_id                                        AS customer_id,
             CASE
                 WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
                 WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
                 END                                              AS revenue_type,
             coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
             coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
             coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                                              AS interval_days,
             sp.name                                               AS plan_id,
             NULL                                                 AS region,
             o.source                                             AS channel,
             o.currency                                           AS currency,
             NULL                                                 AS cancellation_reason,
             NULL                                                 AS pause_reason
      FROM dunning_counters d
               JOIN subscriptions s on d.subscription_id = s.id
               JOIN orders o on s.origin_order_id = o.id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE d.type = 'FailedBillingCounter'
        AND d.state = 'closed'
        AND failed_cycles < max_failed_cycles
        AND o.state NOT IN ('cancelled', 'refunded')

      UNION ALL

      SELECT md5(
                     concat_ws(
                             '::',
                             'free-soul-sistas',
                             sl.created_at::text,
                             'order_created',
                             'addon',
                             s.id::text
                     )
             )                                     AS event_id,
             'free-soul-sistas'                    AS tenant_id,
             sl.created_at                         AS event_time,
             'order_created'                       AS event_type,
             'active'                              AS status_after,
             s.id                                  AS subscription_id,
             s.customer_id                         AS customer_id,
             'addon'                               AS revenue_type,
             coalesce((sl.price * 100)::bigint, 0) AS price_cents,
             0                                     AS tax_cents,
             0                                     AS shipping_cents,
             CASE
                 WHEN bp.interval = 'DAY' THEN bp.frequency * 1
                 WHEN bp.interval = 'WEEK' THEN bp.frequency * 7
                 WHEN bp.interval = 'MONTH' THEN bp.frequency * 30
                 WHEN bp.interval = 'YEAR' THEN bp.frequency * 365
                 ELSE 30
                 END                               AS interval_days,
             sp.name                                AS plan_id,
             NULL                                  AS region,
             s.source                              AS channel,
             s.currency_code                       AS currency,
             NULL                                  AS cancellation_reason,
             NULL                                  AS pause_reason
      FROM subscriptions s
               JOIN subscription_lines sl on s.id = sl.subscription_id
               LEFT JOIN billing_policies bp ON bp.subscription_id = s.id
               LEFT JOIN selling_plans sp ON sp.id = bp.selling_plan_id
      WHERE sl.deleted_at is NULL
        and sl.one_off = true) AS combined_events
ORDER BY event_time DESC;
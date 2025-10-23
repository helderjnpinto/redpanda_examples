SELECT md5(
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
       )                                          AS event_id,
       'free-soul-sistas'                         AS tenant_id,
       o.created_at                               AS event_time,
       'order_created'                            AS event_type,
       'active'                                   AS status_after, --TODO: 'active' para onetime ?
       so.subscription_id                         AS subscription_id,
       o.customer_id                              AS customer_id,
       CASE
           WHEN 'Subscription Recurring Order' = ANY (o.tags) THEN 'recurring'
           WHEN 'Subscription First Order' = ANY (o.tags) THEN 'first'
           WHEN subscription_id is NULL THEN 'onetime'
           END                                    AS revenue_type,
       coalesce((o.subtotal_price * 100)::bigint, 0)        AS price_cents,
       coalesce((o.total_tax * 100)::bigint, 0)             AS tax_cents,
       coalesce((s.delivery_price_amount * 100)::bigint, 0) AS shipping_cents,
       CASE
           WHEN subscription_id is NOT NULL THEN 30
           ELSE 0 -- onetime
           END                                    AS interval_days,
       NULL                                       AS plan_id,
       NULL                                       AS region,
       o.source                                   AS channel,
       o.currency                                 AS currency,
       NULL                                       AS cancellation_reason,
       NULL                                       AS pause_reason
FROM orders o
         LEFT JOIN subscription_orders so
                   ON o.id = so.order_id
         LEFT JOIN subscriptions s
                   ON so.subscription_id = s.id
WHERE o.state NOT IN ('cancelled', 'refunded');

select revenue_type, count(*)
from orders_events
group by revenue_type;

select *
from orders_events
where revenue_type = 'addon';

select event_id, count(*) as n
from orders_events
group by event_id
order by n desc;

--select tags, count(*) from orders where state != 'cancelled'
--group by tags;

select *
from orders_events
where subscription_id IS NULL;

select *
from subscription_lines
where subscription_id = '45af879d-570a-4993-a0e7-d22417107b71';
select *
from orders o
         join subscriptions s on o.customer_id = s.customer_id
where s.id = '45af879d-570a-4993-a0e7-d22417107b71';

select state, count(*)
from orders
group by state;

--
-- SELECT
--     CASE
--         WHEN 'Subscription Recurring Order' = ANY(o.tags) THEN 'recurring'
--         WHEN 'Subscription First Order' = ANY(o.tags) THEN 'first'
--         ELSE 'other'
--         END AS revenue_type,
--     count(*)
-- FROM orders o
-- where o.state != 'cancelled'
-- group by revenue_type;

-- select *, unnest(tags) as tag from orders
-- where id = '0001b2c5-beab-4269-a138-6b2f85791e02';
--
-- SELECT o.*, t AS tag
-- FROM orders o
--          CROSS JOIN LATERAL unnest(o.tags) AS t
-- WHERE t IN ('Subscription Recurring Order', 'Subscription First Order') AND o.id = '00003a97-b358-4e6f-96c4-338359ec2335';

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Use analytics database
USE analytics;

-- events_raw table
CREATE TABLE IF NOT EXISTS
    events_raw (
                   tenant_id String COMMENT 'Yari Flow tenant identifier (e.g. Shopify shop ID)',
                   event_time DateTime COMMENT 'Timestamp when the event occurred',
                   event_type Enum8 (
                       'order_created' = 1,
                       'sub_created' = 2,
                       'sub_cancelled' = 3,
                       'sub_paused' = 4,
                       'sub_resumed' = 5,
                       'dunning_entered' = 6,
                       'dunning_recovered' = 7,
                       'sub_reactivated' = 8, -- From cancelled to active
                       'paused_exited' = 9, -- Automatic exit from pause to active
                       'dunning_exited' = 10, -- Exit dunning without recovery (e.g., to cancelled)
                       'sub_addition' = 11, -- Adding an item to an existing subscription
                       'sub_removal' = 12 -- Removing an item from an existing subscription
                       ) COMMENT 'Categorizes the event',
                   status_after Enum8 (
                       'active' = 1,
                       'paused' = 2,
                       'dunning' = 3,
                       'cancelled' = 4
                       ) DEFAULT 'active' COMMENT 'Subscription status after this event',
                   subscription_id String COMMENT 'Subscription contract ID',
                   customer_id String COMMENT 'Customer ID',
                   revenue_type Enum8 (
                       'first' = 1,
                       'recurring' = 2,
                       'addon' = 3,
                       'onetime' = 4
                       ) DEFAULT 'recurring',
                   price_cents UInt64 DEFAULT 0 COMMENT 'Subscription price at time of event (enrich for non-orders)',
                   shipping_cents UInt64 DEFAULT 0,
                   tax_cents UInt64 DEFAULT 0,
                   interval_days UInt16 DEFAULT 30,
                   plan_id String,
                   region LowCardinality (String),
                   channel LowCardinality (String),
                   currency String,
                   cancellation_reason LowCardinality (String) DEFAULT '',
                   pause_reason LowCardinality (String) DEFAULT ''
) ENGINE = MergeTree ()
      PARTITION BY
          (tenant_id, toYYYYMM (event_time))
      ORDER BY
          (
           tenant_id,
           event_time,
           event_type,
           subscription_id
              ) TTL event_time + INTERVAL 90 DAY;
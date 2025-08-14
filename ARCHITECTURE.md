# Shopify Analytics Pipeline Architecture

This document provides a detailed overview of the Shopify Analytics Pipeline architecture.

## System Overview

The Shopify Analytics Pipeline is designed to ingest, process, and analyze Shopify event data at scale. The system follows a modern event-driven architecture pattern, using specialized components for each stage of the data pipeline.

## Multi-Tenant Design

The pipeline is designed to handle data from multiple shops in a multi-tenant environment:

1. **Data Isolation**: Each event includes a `shop_id` field that is preserved throughout the pipeline
2. **Partitioned Storage**: ClickHouse tables are partitioned by date and ordered by shop_id for efficient querying
3. **Shop-Specific Views**: Dynamic views are created for each shop to simplify data access
4. **Access Control**: Data can be accessed through shop-specific views to enforce tenant isolation

## Components

### 1. Data Sources

**Shopify Events (simulated via Data Generator)**
- Events include: product creations/updates, order creations/updates, subscription creations/updates
- Each event contains detailed information in JSON format including shop_id for tenant identification
- Events are published to RabbitMQ with appropriate routing keys that include the shop_id

### 2. Message Queue (RabbitMQ)

**Purpose:** Decouples event producers from consumers, providing buffering and reliable delivery
- Configured with durable queues for data persistence
- Uses topic-based routing for event categorization with shop-specific routing keys
- Exposes a management interface for monitoring and administration

### 3. Stream Processing (Redpanda Connect)

**Purpose:** Transforms raw event data into analytics-ready formats
- Consumes messages from RabbitMQ
- Uses Bloblang for data transformation:
  - Flattens nested JSON structures
  - Extracts relevant fields
  - Performs calculations (e.g., price aggregations)
  - Adds metadata and timestamps
  - Preserves shop_id for multi-tenant data isolation
- Routes transformed data to appropriate ClickHouse tables
- Provides a dashboard for monitoring pipeline performance

### 4. Analytics Database (ClickHouse)

**Purpose:** Stores and processes analytical queries on event data
- Optimized for OLAP (Online Analytical Processing) workloads
- Uses column-oriented storage for efficient analytics
- Provides materialized views for common aggregations
- Tables organized by event type (products, orders, subscriptions)
- Tables partitioned by date and ordered by shop_id for efficient multi-tenant queries
- Provides shop-specific views for data isolation and simplified access

## Data Flow

1. **Event Generation**
   - Shopify events are generated with shop_id (in production, these would come from actual Shopify stores)
   - Events are published to RabbitMQ with shop-specific routing keys

2. **Event Ingestion**
   - Redpanda Connect consumes events from RabbitMQ
   - Events are parsed and validated
   - Shop_id is extracted and preserved in metadata

3. **Data Transformation**
   - Redpanda Connect transforms the events using Bloblang
   - Complex nested structures are flattened
   - Relevant fields are extracted
   - Calculated fields are added
   - Shop_id is preserved for tenant isolation

4. **Data Storage**
   - Transformed data is routed to appropriate ClickHouse tables
   - Data is stored in a columnar format optimized for analytics
   - Tables are partitioned by date and ordered by shop_id

5. **Data Analysis**
   - ClickHouse provides fast analytical queries
   - Materialized views offer pre-aggregated data for common queries
   - Shop-specific views provide isolated access to tenant data
   - Custom SQL queries can be run for ad-hoc analysis

## Schema Design

### RabbitMQ

- Exchange: `shopify`
- Queue: `shopify_events`
- Routing Keys:
  - `{shop_id}.product_created`, `{shop_id}.product_updated`
  - `{shop_id}.order_created`, `{shop_id}.order_updated`
  - `{shop_id}.subscription_created`, `{shop_id}.subscription_updated`

### ClickHouse

**Database:** `analytics`

**Tables:**
1. `products` - Stores product information
   - Key fields: shop_id, product_id, title, vendor, product_type, variants_count, min_price, max_price
   - Sorted by: shop_id, product_id, updated_at
   - Partitioned by: month (YYYYMM)

2. `orders` - Stores order information
   - Key fields: shop_id, order_id, customer_id, total_price, financial_status, fulfillment_status
   - Sorted by: shop_id, order_id, updated_at
   - Partitioned by: month (YYYYMM)

3. `subscriptions` - Stores subscription information
   - Key fields: shop_id, subscription_id, customer_id, status, next_billing_date, price
   - Sorted by: shop_id, subscription_id, updated_at
   - Partitioned by: month (YYYYMM)

4. `events_raw` - Stores any unprocessed or unknown events
   - Key fields: shop_id, event_data, event_type, event_timestamp
   - Sorted by: shop_id, event_type, event_timestamp
   - Partitioned by: month (YYYYMM)

**Materialized Views:**
1. `product_daily_stats` - Daily aggregations of product data by shop
2. `order_daily_stats` - Daily aggregations of order data by shop
3. `subscription_daily_stats` - Daily aggregations of subscription data by shop

**Shop-Specific Views:**
- Dynamic views created for each shop (e.g., `shop_{shop_id}_products`)
- Created using the `create_shop_views` function
- Provide isolated access to shop-specific data

## Scaling Considerations

The architecture is designed to scale horizontally:

1. **RabbitMQ**
   - Can be deployed as a cluster for high availability
   - Supports multiple consumers for parallel processing

2. **Redpanda Connect**
   - Stateless design allows for multiple instances
   - Can be scaled based on processing load

3. **ClickHouse**
   - Supports distributed tables across multiple nodes
   - Sharding for horizontal scaling
   - Replication for high availability
   - Efficient multi-tenant queries through partitioning and ordering

## Monitoring and Management

1. **RabbitMQ Management UI**
   - Queue statistics and health
   - Message rates and consumer status

2. **Redpanda Connect Dashboard**
   - Pipeline metrics
   - Throughput and latency monitoring
   - Error rates and processing statistics

3. **ClickHouse**
   - System tables for query performance
   - Monitoring tables for resource usage

## Future Enhancements

1. **Additional Data Sources**
   - Integration with other e-commerce platforms
   - Direct Shopify API integration

2. **Advanced Analytics**
   - Machine learning for demand forecasting
   - Anomaly detection for fraud prevention

3. **Visualization Layer**
   - Integration with Grafana or Superset
   - Custom dashboards for business metrics

4. **Data Enrichment**
   - Geo-location data
   - Customer segmentation
   - Product categorization

5. **Enhanced Multi-Tenancy**
   - Role-based access control for shop data
   - Tenant-specific resource allocation
   - Tenant performance isolation 
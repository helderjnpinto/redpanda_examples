# Shopify Analytics Pipeline

A complete data pipeline solution for ingesting Shopify events from RabbitMQ, transforming them with Redpanda Connect, and storing them in ClickHouse for analytics.

## Architecture

This project demonstrates a complete data pipeline for processing Shopify events:

1. **Data Source**: RabbitMQ receives events from Shopify stores (product updates, orders, subscriptions)
2. **Data Processing**: Redpanda Connect (a stream processor) consumes from RabbitMQ, transforms data using Bloblang, and outputs to ClickHouse
3. **Data Storage**: ClickHouse stores the transformed data in optimized tables for analytics
4. **Data Generation**: A Python script simulates Shopify events for testing

## Components

- **RabbitMQ**: Message queue that receives Shopify events
- **Redpanda Connect**: Stream processor that transforms data
- **ClickHouse**: Column-oriented analytics database
- **Data Generator**: Python script that simulates Shopify events

## Multi-Tenant Architecture

This pipeline is designed to handle data from multiple shops in a multi-tenant environment:

1. **Data Isolation**: Each event includes a `shop_id` field to identify the source shop
2. **Partitioned Storage**: ClickHouse tables are partitioned by date and ordered by shop_id for efficient querying
3. **Shop-Specific Views**: Dynamic views are created for each shop to simplify data access
4. **Routing**: Events are routed with shop-specific keys in RabbitMQ (`{shop_id}.{event_type}`)

## Getting Started

### Prerequisites

- Docker
- Docker Compose

### Running the Pipeline

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/shopify-analytics.git
   cd shopify-analytics
   ```

2. Start all services:
   ```
   docker-compose up -d
   ```

3. Monitor the logs:
   ```
   docker-compose logs -f
   ```

4. Access the services:
   - RabbitMQ Management: http://localhost:15672 (user/password)
   - Redpanda Connect Dashboard: http://localhost:4195
   - ClickHouse HTTP interface: http://localhost:8123

### Exploring the Data

1. Connect to ClickHouse:
   ```
   docker-compose exec clickhouse clickhouse-client
   ```

2. Run some sample queries:
   ```sql
   -- Switch to analytics database
   USE analytics;
   
   -- Count products by type and shop
   SELECT shop_id, product_type, COUNT(*) as count
   FROM products
   GROUP BY shop_id, product_type
   ORDER BY shop_id, count DESC;
   
   -- Get revenue by day and shop
   SELECT 
       shop_id,
       toDate(created_at) as date,
       sum(total_price) as daily_revenue
   FROM orders
   GROUP BY shop_id, date
   ORDER BY shop_id, date DESC;
   
   -- Subscription metrics by shop
   SELECT 
       shop_id,
       status,
       COUNT(*) as count,
       sum(price * quantity) as total_value
   FROM subscriptions
   GROUP BY shop_id, status;
   
   -- Create shop-specific views
   SELECT create_shop_views('shop_1001');
   
   -- Query shop-specific view
   SELECT * FROM shop_shop_1001_orders LIMIT 10;
   ```

## Customization

### Adding New Event Types

1. Update the data generator in `data-generator/generator.py`
2. Add transformation logic in `redpanda-config/pipeline.yaml`
3. Create a new table in ClickHouse by modifying `clickhouse-config/init-db.sql`

### Scaling the Pipeline

- Increase the number of Redpanda Connect instances
- Add more RabbitMQ nodes in a cluster
- Scale ClickHouse with additional replicas

### Adding New Shops

1. Add new shop IDs to the `SHOP_IDS` list in `data-generator/generator.py`
2. Create shop-specific views in ClickHouse:
   ```sql
   SELECT create_shop_views('new_shop_id');
   ```

## Architecture Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│             │     │             │     │             │     │             │
│   Shopify   │────▶│   RabbitMQ  │────▶│  Redpanda   │────▶│  ClickHouse │
│   Events    │     │             │     │   Connect   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

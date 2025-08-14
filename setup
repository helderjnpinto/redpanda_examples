#!/bin/bash
set -e

echo "Setting up Shopify Analytics Pipeline..."

# Create necessary directories
mkdir -p redpanda-config
mkdir -p clickhouse-config
mkdir -p data-generator

# Make scripts executable
chmod +x clickhouse-config/init-db.sh
chmod +x data-generator/generator.py

# Start the services
echo "Starting all services with Docker Compose..."
docker-compose up -d

echo "Waiting for services to initialize (this may take a minute)..."
sleep 30

echo "Checking service health..."

# Check RabbitMQ
if docker-compose exec rabbitmq rabbitmqctl status > /dev/null 2>&1; then
  echo "✅ RabbitMQ is running"
else
  echo "❌ RabbitMQ is not running properly"
fi

# Check Redpanda Connect
if docker-compose exec benthos redpanda-connect -version > /dev/null 2>&1; then
  echo "✅ Redpanda Connect is running"
else
  echo "❌ Redpanda Connect is not running properly"
fi

# Check ClickHouse
if docker-compose exec clickhouse clickhouse-client --query="SELECT 1" > /dev/null 2>&1; then
  echo "✅ ClickHouse is running"
else
  echo "❌ ClickHouse is not running properly"
fi

echo ""
echo "Setup complete! You can now access:"
echo "- RabbitMQ Management: http://localhost:15672 (user/password)"
echo "- Redpanda Connect Dashboard: http://localhost:4195"
echo "- ClickHouse HTTP interface: http://localhost:8123"
echo ""
echo "To view logs: docker-compose logs -f"
echo "To stop all services: docker-compose down" 
#!/bin/bash
set -e

# Wait for ClickHouse to be ready
until clickhouse-client --host=clickhouse --query="SELECT 1"; do
  echo "Waiting for ClickHouse to start..."
  sleep 2
done

echo "ClickHouse is up and running. Initializing database..."

# Execute the SQL schema
clickhouse-client --host=clickhouse --multiquery < /etc/clickhouse-server/config.d/init-db.sql

echo "Database initialization completed." 
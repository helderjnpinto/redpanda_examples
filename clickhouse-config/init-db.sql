-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- Use analytics database
USE analytics;

-- events_raw table
CREATE TABLE IF NOT EXISTS events_raw (
    id Int64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY (id);
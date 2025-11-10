-- Create the database
CREATE DATABASE pulseiq_api_db;

-- Connect to the new database
\c pulseiq_api_db;

-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create our first table for time-series rollups
CREATE TABLE topic_sentiment_rollups (
    time TIMESTAMPTZ NOT NULL,
    topic VARCHAR(255) NOT NULL,
    sentiment_label VARCHAR(50) NOT NULL,
    sentiment_count INT NOT NULL,
    -- We can add more dimensions here later, like country_code
    PRIMARY KEY (time, topic, sentiment_label)
);

-- Turn our table into a TimescaleDB hypertable
SELECT create_hypertable('topic_sentiment_rollups', 'time');

-- Create an index for fast lookups
CREATE INDEX ON topic_sentiment_rollups (topic, time DESC);

-- (We can add geo-rollups and other tables here later)
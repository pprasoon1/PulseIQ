-- Create the database
CREATE DATABASE pulseiq_api_db;

-- Connect to the new database
\c pulseiq_api_db;

-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create our table for time-series & geo rollups
CREATE TABLE topic_sentiment_rollups (
    time TIMESTAMPTZ NOT NULL,
    topic VARCHAR(255) NOT NULL,
    sentiment_label VARCHAR(50) NOT NULL,
    country_code VARCHAR(3) NOT NULL, -- NEW COLUMN
    sentiment_count INT NOT NULL,
    PRIMARY KEY (time, topic, sentiment_label, country_code) -- UPDATED KEY
);

-- Turn our table into a TimescaleDB hypertable
SELECT create_hypertable('topic_sentiment_rollups', 'time');

-- Create an index for fast lookups (topic, time, country)
CREATE INDEX ON topic_sentiment_rollups (topic, time DESC, country_code);
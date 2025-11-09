-- TimescaleDB setup for Kraken order book messages
-- Run this with: docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres" -f /path/to/this/file

-- Create the main table for order book messages
CREATE TABLE IF NOT EXISTS kraken_orderbook_messages (
    id SERIAL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    channel TEXT NOT NULL,
    message_type TEXT NOT NULL,  -- 'snapshot' or 'update'
    symbol TEXT NOT NULL,
    checksum BIGINT,
    raw_message JSONB NOT NULL,  -- Store the complete original message
    PRIMARY KEY (timestamp, symbol, id)
);

-- Convert to hypertable (TimescaleDB specific)
SELECT create_hypertable('kraken_orderbook_messages', 'timestamp', if_not_exists => TRUE);

-- Create table for individual bid/ask entries (normalized)
CREATE TABLE IF NOT EXISTS kraken_orderbook_entries (
    timestamp TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,  -- 'bid' or 'ask'
    price NUMERIC(20,8) NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    message_type TEXT NOT NULL,  -- 'snapshot' or 'update'
    checksum BIGINT,
    PRIMARY KEY (timestamp, symbol, side, price)
);

-- Convert to hypertable
SELECT create_hypertable('kraken_orderbook_entries', 'timestamp', if_not_exists => TRUE);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_orderbook_messages_symbol_type ON kraken_orderbook_messages(symbol, message_type);
CREATE INDEX IF NOT EXISTS idx_orderbook_entries_symbol_side ON kraken_orderbook_entries(symbol, side);
CREATE INDEX IF NOT EXISTS idx_orderbook_messages_timestamp ON kraken_orderbook_messages(timestamp);
CREATE INDEX IF NOT EXISTS idx_orderbook_entries_timestamp ON kraken_orderbook_entries(timestamp);

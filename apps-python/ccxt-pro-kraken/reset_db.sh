#!/bin/bash

# Reset TimescaleDB tables for Kraken order book data
# This script empties all tables so you can start fresh

echo "Resetting Kraken order book database tables..."

# Connect to TimescaleDB and truncate tables
docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres" << 'EOF'

-- Truncate all data from the tables (preserves table structure and hypertables)
TRUNCATE TABLE kraken_orderbook_messages;
TRUNCATE TABLE kraken_orderbook_entries;

-- Show table counts to confirm they're empty
SELECT 'kraken_orderbook_messages' as table_name, COUNT(*) as record_count FROM kraken_orderbook_messages
UNION ALL
SELECT 'kraken_orderbook_entries' as table_name, COUNT(*) as record_count FROM kraken_orderbook_entries;

-- Show hypertable status (should still exist)
SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables WHERE hypertable_name LIKE 'kraken%';

EOF

echo "Database reset complete! All order book data has been cleared."
echo "Tables structure and hypertables remain intact."
echo "You can now run your order book collector as if starting fresh."

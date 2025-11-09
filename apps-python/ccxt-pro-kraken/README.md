# Kraken Order Book Data Collector

This project captures Kraken WebSocket order book messages and stores them in TimescaleDB for analysis and replay.

## Setup Steps

### 1. Install Dependencies
```bash
pip install asyncpg
```

Or install from requirements file:
```bash
pip install -r requirements.txt
```

### 2. Setup Database Tables

First, make sure your TimescaleDB container is running, then setup the database tables.

**Option 1: Copy file to container and execute**
```bash
# Copy the SQL file into the container
docker cp setup_timescale_tables.sql timescaledb:/tmp/setup_timescale_tables.sql

# Execute the SQL file
docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres" -f /tmp/setup_timescale_tables.sql
```

**Option 2: Execute SQL commands manually**
```bash
docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres"
```

Then copy and paste the SQL commands from `setup_timescale_tables.sql`.

## What the Enhanced Code Does

### Database Connection
- Creates an async connection pool to TimescaleDB
- Handles connection errors gracefully

### Raw Message Storage
- Saves the complete original message as JSONB for exact replay
- Preserves all original data structure and metadata

### Normalized Data
- Also stores individual bid/ask entries in a separate table for easier querying
- Allows for efficient price/quantity analysis

### Timestamp Handling
- Uses the message timestamp when available from Kraken
- Falls back to current time if timestamp not provided

### Error Handling
- Catches and logs database errors without breaking the main WebSocket flow
- Continues operation even if database writes fail

### Conflict Resolution
- Uses ON CONFLICT for bid/ask entries to handle duplicate timestamps
- Ensures data integrity

## Benefits

- **Exact Replay**: You can reconstruct the exact sequence of events
- **Efficient Queries**: TimescaleDB is optimized for time-series data
- **Flexible Analysis**: Both raw messages and normalized data available
- **Data Integrity**: Checksums stored for verification
- **High Performance**: Async operations don't block WebSocket processing

## Usage

### 1. Collect Data
Run the main script to start collecting order book data:
```bash
python watch_order_book.py
```

### 2. View Collected Data
Use the query script to view the collected data:
```bash
python query_data.py
```

### 3. Custom Analysis
Query the database directly for custom analysis:
```bash
docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres"
```

#### Basic Database Inspection Commands
```sql
-- List all tables
\dt

-- Describe table structure
\d kraken_orderbook_messages
\d kraken_orderbook_entries

-- List all databases
\l

-- List all schemas
\dn
```

#### Data Inspection Queries
```sql
-- Check if tables exist and have data
SELECT COUNT(*) FROM kraken_orderbook_messages;
SELECT COUNT(*) FROM kraken_orderbook_entries;

-- View recent messages
SELECT * FROM kraken_orderbook_messages ORDER BY timestamp DESC LIMIT 5;

-- View recent bid/ask entries
SELECT * FROM kraken_orderbook_entries ORDER BY timestamp DESC LIMIT 10;

-- Check unique symbols
SELECT DISTINCT symbol FROM kraken_orderbook_messages;

-- Get message statistics
SELECT 
    COUNT(*) as total_messages,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(timestamp) as first_message,
    MAX(timestamp) as last_message
FROM kraken_orderbook_messages;

-- View raw JSON message (pretty formatted)
SELECT jsonb_pretty(raw_message) FROM kraken_orderbook_messages LIMIT 1;
```

#### TimescaleDB Specific Queries
```sql
-- Check hypertables
SELECT * FROM timescaledb_information.hypertables;

-- Check chunks
SELECT * FROM timescaledb_information.chunks;

-- Exit psql
\q
```

Example queries:
```sql
-- View recent messages
SELECT * FROM kraken_orderbook_messages ORDER BY timestamp DESC LIMIT 10;

-- View bid/ask entries for specific symbol
SELECT * FROM kraken_orderbook_entries WHERE symbol = 'BTC/USD' ORDER BY timestamp DESC;

-- Get message statistics
SELECT
    COUNT(*) as total_messages,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(timestamp) as first_message,
    MAX(timestamp) as last_message
FROM kraken_orderbook_messages;
```

## File Structure

- `watch_order_book.py` - Main script with custom Kraken class
- `setup_timescale_tables.sql` - Database schema setup
- `query_data.py` - Helper script to view collected data
- `requirements.txt` - Python dependencies
- `README.md` - This file

## Data Storage

The data is stored with microsecond precision timestamps in two tables:

1. **kraken_orderbook_messages** - Raw WebSocket messages
2. **kraken_orderbook_entries** - Normalized bid/ask entries

This allows you to replay the exact sequence of order book events you received while also enabling efficient analysis of price movements.

## Configuration

The database connection string in the code assumes:
- Host: localhost
- Database: postgres
- Username: postgres  
- Password: password

Modify the connection string in `watch_order_book.py` if your setup is different:
```python
self.db_pool = await asyncpg.create_pool(
    "postgresql://username:password@host:port/database",
    min_size=1,
    max_size=5
)
```

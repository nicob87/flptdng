# Kraken Replay Server Usage Guide

## Installation

First install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### 1. Start the Replay Server

```bash
python replay_server.py
```

The server will start on `http://localhost:8080` by default. You can specify a different port:

```bash
python replay_server.py --port 8888
```

### 2. Watch Order Book (Live Mode)

To connect to the real Kraken and collect data:

```bash
python watch_order_book.py
```

### 3. Watch Order Book (Replay Mode) 

To replay from a specific date:

```bash
# Replay from a specific date
python watch_order_book.py -s "2025-11-08"

# Replay from a specific datetime
python watch_order_book.py -s "2025-11-08T17:30:00"

# Use custom replay server port
python watch_order_book.py -s "2025-11-08" --port 8888
```

## API Endpoints

### Prepare Replay (HTTP POST)

```bash
curl -X POST http://localhost:8080/replay/prepare \
  -H "Content-Type: application/json" \
  -d '{"date": "2025-11-08T17:00:00"}'
```

Response:
```json
{
  "status": "ready",
  "replay_start_timestamp": "2025-11-08T17:30:15.123456+00:00",
  "requested_date": "2025-11-08T17:00:00+00:00",
  "message": "Replay prepared. Connect via WebSocket to start."
}
```

### WebSocket Replay

Connect to: `ws://localhost:8080/ws?start_date=2025-11-08T17:30:15.123456+00:00`

The server will replay all order book messages starting from the specified timestamp in chronological order, following the exact same format as Kraken's WebSocket API.

## Replay Behavior

1. **Snapshot Detection**: The server finds the first snapshot message after your requested date
2. **Sequential Replay**: Messages are sent in chronological order
3. **Snapshot Interruption**: If a new snapshot is encountered during replay of updates, the replay stops
4. **Kraken Format**: Messages are sent in the exact same format as Kraken's WebSocket API

## Examples

### Collecting Data
```bash
# Start collecting live data
python watch_order_book.py

# Let it run for a while to collect data...
# Then stop with Ctrl+C
```

### Start Replay Server
```bash
python replay_server.py
```

### Replay from Yesterday
```bash
python watch_order_book.py -s "2025-11-08"
```

## Troubleshooting

- **No data found**: Make sure you have collected data first using live mode
- **Connection failed**: Ensure the replay server is running
- **Import errors**: Install dependencies with `pip install -r requirements.txt`
- **Database errors**: Make sure TimescaleDB is running and accessible

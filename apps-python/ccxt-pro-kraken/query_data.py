import asyncpg
import asyncio
import json

async def query_orderbook_data():
    """Query and display saved order book data"""
    conn = await asyncpg.connect("postgresql://postgres:password@localhost/postgres")
    
    try:
        print("=== Recent Order Book Messages ===")
        messages = await conn.fetch("""
            SELECT timestamp, channel, message_type, symbol, checksum, raw_message
            FROM kraken_orderbook_messages
            ORDER BY timestamp DESC
            LIMIT 10
        """)
        
        for msg in messages:
            print(f"Timestamp: {msg['timestamp']}")
            print(f"Type: {msg['message_type']}, Symbol: {msg['symbol']}")
            print(f"Checksum: {msg['checksum']}")
            print(f"Raw message: {json.dumps(json.loads(msg['raw_message']), indent=2)}")
            print("-" * 80)
        
        print("\n=== Recent Bid/Ask Entries ===")
        entries = await conn.fetch("""
            SELECT timestamp, symbol, side, price, quantity, message_type
            FROM kraken_orderbook_entries
            WHERE symbol = 'BTC/USD'
            ORDER BY timestamp DESC, side, price
            LIMIT 20
        """)
        
        for entry in entries:
            print(f"{entry['timestamp']} | {entry['symbol']} | {entry['side']} | "
                  f"${entry['price']} x {entry['quantity']} | {entry['message_type']}")
            
        print(f"\n=== Statistics ===")
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(timestamp) as first_message,
                MAX(timestamp) as last_message
            FROM kraken_orderbook_messages
        """)
        
        print(f"Total messages: {stats['total_messages']}")
        print(f"Unique symbols: {stats['unique_symbols']}")
        print(f"First message: {stats['first_message']}")
        print(f"Last message: {stats['last_message']}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(query_orderbook_data())

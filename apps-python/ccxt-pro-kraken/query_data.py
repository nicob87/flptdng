import asyncpg
import asyncio
import json
import argparse

async def query_orderbook_data(sort_order='DESC', count=10, data_type='all'):
    """Query and display saved order book data"""
    conn = await asyncpg.connect("postgresql://postgres:password@localhost/postgres")
    
    # Validate sort order
    order_by = "ASC" if sort_order.lower() == 'a' else "DESC"
    
    try:
        if data_type in ['all', 'messages']:
            print("=== Recent Order Book Messages ===")
            messages = await conn.fetch(f"""
                SELECT timestamp, channel, message_type, symbol, checksum, raw_message
                FROM kraken_orderbook_messages
                ORDER BY timestamp {order_by}
                LIMIT $1
            """, count)
            
            for msg in messages:
                print(f"Timestamp: {msg['timestamp']}")
                print(f"Type: {msg['message_type']}, Symbol: {msg['symbol']}")
                print(f"Checksum: {msg['checksum']}")
                print(f"Raw message: {json.dumps(json.loads(msg['raw_message']), indent=2)}")
                print("-" * 80)
        
        if data_type in ['all', 'entries']:
            print("\n=== Recent Bid/Ask Entries ===")
            entries = await conn.fetch(f"""
                SELECT timestamp, symbol, side, price, quantity, message_type
                FROM kraken_orderbook_entries
                WHERE symbol = 'BTC/USD'
                ORDER BY timestamp {order_by}, side, price
                LIMIT $1
            """, count)
            
            for entry in entries:
                print(f"{entry['timestamp']} | {entry['symbol']} | {entry['side']} | "
                      f"${entry['price']} x {entry['quantity']} | {entry['message_type']}")
        
        # Always show statistics
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

def main():
    parser = argparse.ArgumentParser(description='Query Kraken order book data from TimescaleDB')
    
    parser.add_argument('-s', '--sort', 
                        choices=['a', 'd'], 
                        default='d',
                        help='Sorting order: a=ascending, d=descending (default: d)')
    
    parser.add_argument('-c', '--count', 
                        type=int, 
                        default=10,
                        help='Number of samples to retrieve (default: 10)')
    
    # Create mutually exclusive group for data type
    data_group = parser.add_mutually_exclusive_group()
    data_group.add_argument('-m', '--messages', 
                           action='store_const', 
                           const='messages', 
                           dest='data_type',
                           help='Show messages only')
    
    data_group.add_argument('-e', '--entries', 
                           action='store_const', 
                           const='entries', 
                           dest='data_type',
                           help='Show entries only')
    
    data_group.add_argument('-a', '--all', 
                           action='store_const', 
                           const='all', 
                           dest='data_type',
                           help='Show both messages and entries (default)')
    
    # Set default for data_type if not specified
    parser.set_defaults(data_type='all')
    
    args = parser.parse_args()
    
    print(f"Querying with: sort={args.sort}, count={args.count}, data_type={args.data_type}")
    
    asyncio.run(query_orderbook_data(args.sort, args.count, args.data_type))

if __name__ == "__main__":
    main()

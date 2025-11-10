#!/usr/bin/env python3

import asyncpg
import asyncio

async def reset_database():
    """Reset (empty) all Kraken order book database tables"""
    print("Resetting Kraken order book database tables...")
    
    try:
        # Connect to the database
        conn = await asyncpg.connect("postgresql://postgres:password@localhost/postgres")
        
        # Truncate all data from the tables (preserves table structure and hypertables)
        print("Truncating kraken_orderbook_messages...")
        await conn.execute("TRUNCATE TABLE kraken_orderbook_messages")
        
        print("Truncating kraken_orderbook_entries...")
        await conn.execute("TRUNCATE TABLE kraken_orderbook_entries")
        
        # Show table counts to confirm they're empty
        print("\nVerifying tables are empty:")
        
        msg_count = await conn.fetchval("SELECT COUNT(*) FROM kraken_orderbook_messages")
        entries_count = await conn.fetchval("SELECT COUNT(*) FROM kraken_orderbook_entries")
        
        print(f"kraken_orderbook_messages: {msg_count} records")
        print(f"kraken_orderbook_entries: {entries_count} records")
        
        # Show hypertable status (should still exist)
        print("\nHypertable status:")
        hypertables = await conn.fetch("""
            SELECT hypertable_name, num_chunks 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_name LIKE 'kraken%'
        """)
        
        for ht in hypertables:
            print(f"  {ht['hypertable_name']}: {ht['num_chunks']} chunks")
        
        await conn.close()
        
        print("\n✅ Database reset complete!")
        print("📊 All order book data has been cleared.")
        print("🏗️  Tables structure and hypertables remain intact.")
        print("🚀 You can now run your order book collector as if starting fresh.")
        
    except Exception as e:
        print(f"❌ Error resetting database: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(reset_database())
    exit(0 if success else 1)

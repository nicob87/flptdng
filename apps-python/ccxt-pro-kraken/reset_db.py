#!/usr/bin/env python3

import asyncpg
import asyncio


async def setup_tables_from_scratch(conn):
    """Setup database tables from scratch if they don't exist"""
    print("Setting up database tables from scratch...")

    try:
        # Create the main table for order book messages
        print("Creating kraken_orderbook_messages table...")
        await conn.execute(
            """
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
            )
        """
        )

        # Create hypertable for messages
        print("Creating hypertable for kraken_orderbook_messages...")
        await conn.execute(
            """
            SELECT create_hypertable('kraken_orderbook_messages', 'timestamp', if_not_exists => TRUE)
        """
        )

        # Create table for individual bid/ask entries
        print("Creating kraken_orderbook_entries table...")
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kraken_orderbook_entries (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,  -- 'bid' or 'ask'
                price NUMERIC(20,8) NOT NULL,
                quantity NUMERIC(20,8) NOT NULL,
                message_type TEXT NOT NULL,  -- 'snapshot' or 'update'
                checksum BIGINT,
                PRIMARY KEY (timestamp, symbol, side, price)
            )
        """
        )

        # Create hypertable for entries
        print("Creating hypertable for kraken_orderbook_entries...")
        await conn.execute(
            """
            SELECT create_hypertable('kraken_orderbook_entries', 'timestamp', if_not_exists => TRUE)
        """
        )

        # Create indexes
        print("Creating performance indexes...")
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orderbook_messages_symbol_type 
            ON kraken_orderbook_messages(symbol, message_type)
        """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orderbook_entries_symbol_side 
            ON kraken_orderbook_entries(symbol, side)
        """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orderbook_messages_timestamp 
            ON kraken_orderbook_messages(timestamp)
        """
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_orderbook_entries_timestamp 
            ON kraken_orderbook_entries(timestamp)
        """
        )

        print("✅ Database tables created successfully!")
        return True

    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False


async def check_tables_exist(conn):
    """Check if the required tables exist"""
    try:
        # Check if both required tables exist
        result = await conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('kraken_orderbook_messages', 'kraken_orderbook_entries')
        """
        )

        existing_tables = [row["table_name"] for row in result]
        required_tables = {"kraken_orderbook_messages", "kraken_orderbook_entries"}

        print(f"Found existing tables: {existing_tables}")
        return required_tables.issubset(set(existing_tables))

    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


async def reset_database():
    """Reset (empty) all Kraken order book database tables"""
    print("Starting database reset/setup process...")

    try:
        # Connect to the database
        conn = await asyncpg.connect(
            "postgresql://postgres:password@localhost/postgres"
        )

        # Check if tables exist
        tables_exist = await check_tables_exist(conn)

        if not tables_exist:
            print("🏗️  Required tables not found. Setting up database from scratch...")
            success = await setup_tables_from_scratch(conn)
            if not success:
                await conn.close()
                return False
        else:
            print("📊 Tables found. Proceeding to reset data...")

            # Truncate all data from the tables (preserves table structure and hypertables)
            print("Truncating kraken_orderbook_messages...")
            await conn.execute("TRUNCATE TABLE kraken_orderbook_messages")

            print("Truncating kraken_orderbook_entries...")
            await conn.execute("TRUNCATE TABLE kraken_orderbook_entries")

        # Show table counts to confirm they're empty
        print("\nVerifying tables status:")

        msg_count = await conn.fetchval(
            "SELECT COUNT(*) FROM kraken_orderbook_messages"
        )
        entries_count = await conn.fetchval(
            "SELECT COUNT(*) FROM kraken_orderbook_entries"
        )

        print(f"kraken_orderbook_messages: {msg_count} records")
        print(f"kraken_orderbook_entries: {entries_count} records")

        # Show hypertable status (should exist)
        print("\nHypertable status:")
        try:
            hypertables = await conn.fetch(
                """
                SELECT hypertable_name, num_chunks 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name LIKE 'kraken%'
            """
            )

            if hypertables:
                for ht in hypertables:
                    print(f"  {ht['hypertable_name']}: {ht['num_chunks']} chunks")
            else:
                print(
                    "  No hypertables found (this might be normal if not using TimescaleDB)"
                )
        except Exception as e:
            print(
                f"  Could not fetch hypertable info (normal if not using TimescaleDB): {e}"
            )

        await conn.close()

        if not tables_exist:
            print("\n✅ Database setup complete!")
            print("🏗️  All tables and indexes have been created.")
        else:
            print("\n✅ Database reset complete!")
            print("📊 All order book data has been cleared.")

        print("🏗️  Tables structure and hypertables remain intact.")
        print("🚀 You can now run your order book collector as if starting fresh.")

    except Exception as e:
        print(f"❌ Error resetting/setting up database: {e}")
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(reset_database())
    exit(0 if success else 1)

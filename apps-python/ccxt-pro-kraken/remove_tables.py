#!/usr/bin/env python3

import asyncpg
import asyncio


async def remove_all_tables():
    """Remove all Kraken order book database tables and related objects"""
    print("🗑️  Removing all Kraken order book database objects...")

    try:
        # Connect to the database
        conn = await asyncpg.connect(
            "postgresql://postgres:password@localhost/postgres"
        )

        # Check what tables exist first
        print("📋 Checking existing tables...")
        existing_tables = await conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'kraken%'
        """
        )

        if not existing_tables:
            print("ℹ️  No Kraken tables found in the database.")
            await conn.close()
            print("✅ Database is already clean!")
            return True

        print("Found the following Kraken tables:")
        for table in existing_tables:
            print(f"  - {table['table_name']}")

        # Check for hypertables first (TimescaleDB specific)
        print("\n🔍 Checking for TimescaleDB hypertables...")
        try:
            hypertables = await conn.fetch(
                """
                SELECT hypertable_name 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name LIKE 'kraken%'
            """
            )

            if hypertables:
                print("Found TimescaleDB hypertables:")
                for ht in hypertables:
                    print(f"  - {ht['hypertable_name']}")

                # Drop hypertables (this also drops the underlying tables)
                for ht in hypertables:
                    table_name = ht["hypertable_name"]
                    print(f"🗑️  Dropping hypertable: {table_name}")
                    await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            else:
                print("ℹ️  No TimescaleDB hypertables found.")

                # Drop regular tables if no hypertables exist
                for table in existing_tables:
                    table_name = table["table_name"]
                    print(f"🗑️  Dropping table: {table_name}")
                    await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        except Exception as e:
            print(
                f"⚠️  TimescaleDB operations failed (normal if not using TimescaleDB): {e}"
            )
            print("🗑️  Falling back to regular table drops...")

            # Drop regular tables
            for table in existing_tables:
                table_name = table["table_name"]
                print(f"🗑️  Dropping table: {table_name}")
                await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        # Drop any remaining indexes that might be left over
        print("\n🧹 Cleaning up any remaining indexes...")
        try:
            # Get all indexes that start with our naming convention
            indexes = await conn.fetch(
                """
                SELECT indexname 
                FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND indexname LIKE 'idx_orderbook%'
            """
            )

            for idx in indexes:
                index_name = idx["indexname"]
                print(f"🗑️  Dropping index: {index_name}")
                await conn.execute(f"DROP INDEX IF EXISTS {index_name}")

        except Exception as e:
            print(f"⚠️  Index cleanup failed: {e}")

        # Drop any sequences that might be left over
        print("\n🧹 Cleaning up any remaining sequences...")
        try:
            sequences = await conn.fetch(
                """
                SELECT sequence_name 
                FROM information_schema.sequences 
                WHERE sequence_schema = 'public' 
                AND sequence_name LIKE 'kraken%'
            """
            )

            for seq in sequences:
                seq_name = seq["sequence_name"]
                print(f"🗑️  Dropping sequence: {seq_name}")
                await conn.execute(f"DROP SEQUENCE IF EXISTS {seq_name}")

        except Exception as e:
            print(f"⚠️  Sequence cleanup failed: {e}")

        # Verify everything is cleaned up
        print("\n🔍 Verifying cleanup...")
        remaining_tables = await conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'kraken%'
        """
        )

        remaining_indexes = await conn.fetch(
            """
            SELECT indexname 
            FROM pg_indexes 
            WHERE schemaname = 'public' 
            AND indexname LIKE 'idx_orderbook%'
        """
        )

        remaining_sequences = await conn.fetch(
            """
            SELECT sequence_name 
            FROM information_schema.sequences 
            WHERE sequence_schema = 'public' 
            AND sequence_name LIKE 'kraken%'
        """
        )

        if remaining_tables or remaining_indexes or remaining_sequences:
            print("⚠️  Some objects might still remain:")
            for table in remaining_tables:
                print(f"  Table: {table['table_name']}")
            for idx in remaining_indexes:
                print(f"  Index: {idx['indexname']}")
            for seq in remaining_sequences:
                print(f"  Sequence: {seq['sequence_name']}")
        else:
            print("✅ All Kraken objects successfully removed!")

        await conn.close()

        print("\n✅ Database cleanup complete!")
        print(
            "🧹 All Kraken order book tables, indexes, and sequences have been removed."
        )
        print("🌟 Database is now in a clean state as if newly created.")
        print("🚀 You can run reset_db.py to set up tables again when needed.")

    except Exception as e:
        print(f"❌ Error removing database objects: {e}")
        return False

    return True


async def confirm_removal():
    """Ask for user confirmation before removing tables"""
    print(
        "⚠️  WARNING: This will completely remove all Kraken order book data and tables!"
    )
    print("📊 This action cannot be undone.")
    print("🗑️  All collected order book history will be permanently lost.")

    try:
        response = (
            input("\nAre you sure you want to proceed? (type 'yes' to confirm): ")
            .strip()
            .lower()
        )
        return response == "yes"
    except KeyboardInterrupt:
        print("\n🛑 Operation cancelled by user.")
        return False


if __name__ == "__main__":
    print("🗑️  Kraken Database Table Removal Tool")
    print("=" * 50)

    # Ask for confirmation
    if confirm_removal():
        success = asyncio.run(remove_all_tables())
        exit(0 if success else 1)
    else:
        print("🛑 Table removal cancelled.")
        print("💡 No changes were made to the database.")
        exit(0)

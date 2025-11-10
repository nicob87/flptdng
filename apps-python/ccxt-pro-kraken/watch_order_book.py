import ccxt.pro as ccxtpro
from asyncio import run, sleep
import asyncio
import asyncpg
import json
import datetime
from decimal import Decimal

# ===== Order book message: {'channel': 'book', 'type': 'snapshot', 'data': [{'symbol': 'BTC/USD', 'bids': [{'price': 101926.6, 'qty': 0.1830147}, {'price': 101920.0, 'qty': 0.00148254}, {'price': 101910.0, 'qty': 6.378e-05}, {'price': 101908.6, 'qty': 0.00204821}, {'price': 101900.0, 'qty': 0.0052246}, {'price': 101893.9, 'qty': 2.45353086}, {'price': 101892.5, 'qty': 2.4535644}, {'price': 101891.0, 'qty': 6.87e-05}, {'price': 101890.9, 'qty': 2.45360371}, {'price': 101890.8, 'qty': 0.06379541}], 'asks': [{'price': 101926.7, 'qty': 8.53084965}, {'price': 101927.9, 'qty': 0.470968}, {'price': 101928.0, 'qty': 2.45271281}, {'price': 101928.9, 'qty': 1.37388213}, {'price': 101929.0, 'qty': 4.90537575}, {'price': 101929.3, 'qty': 2.45268223}, {'price': 101929.9, 'qty': 6.377e-05}, {'price': 101930.0, 'qty': 0.00051}, {'price': 101934.0, 'qty': 0.38977985}, {'price': 101934.4, 'qty': 4.90511947}], 'checksum': 1992539753}]} {'bids': [[101926.6, 0.1830147], [101920.0, 0.00148254], [101910.0, 6.378e-05], [101908.6, 0.00204821], [101900.0, 0.0052246], [101893.9, 2.45353086], [101892.5, 2.4535644], [101891.0, 6.87e-05], [101890.9, 2.45360371], [101890.8, 0.06379541]], 'asks': [[101926.7, 8.53084965], [101927.9, 0.470968], [101928.0, 2.45271281], [101928.9, 1.37388213], [101929.0, 4.90537575], [101929.3, 2.45268223], [101929.9, 6.377e-05], [101930.0, 0.00051], [101934.0, 0.38977985], [101934.4, 4.90511947]], 'timestamp': None, 'datetime': None, 'nonce': None, 'symbol': 'BTC/USD'}
# ===== Order book message: {'channel': 'book', 'type': 'update',   'data': [{'symbol': 'BTC/USD', 'bids': [{'price': 101892.5, 'qty': 0.0}, {'price': 101890.1, 'qty': 0.470905}], 'asks': [], 'checksum': 1732930167, 'timestamp': '2025-11-08T17:50:22.885395Z'}]} {'bids': [[101926.6, 0.1830147], [101920.0, 0.00148254], [101910.0, 6.378e-05], [101908.6, 0.00204821], [101900.0, 0.0052246], [101891.0, 6.87e-05], [101890.9, 2.45360371], [101890.8, 0.06379541], [101890.4, 2.4536161], [101890.1, 0.470905]], 'asks': [[101926.7, 8.53084965], [101927.9, 0.470968], [101928.0, 2.45271281], [101928.9, 1.37388213], [101929.0, 4.90537575], [101929.3, 2.45268223], [101929.9, 6.377e-05], [101930.0, 0.00051], [101934.0, 0.38977985], [101934.4, 4.90511947]], 'timestamp': 1762624222885, 'datetime': '2025-11-08T17:50:22.885395Z', 'nonce': None, 'symbol': 'BTC/USD'}
# docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres"
class CustomKraken(ccxtpro.kraken):
    def __init__(self, config={}):
        super().__init__(config)
        self.db_pool = None
    
    async def connect_db(self):
        """Initialize database connection pool"""
        if self.db_pool is None:
            self.db_pool = await asyncpg.create_pool(
                "postgresql://postgres:password@localhost/postgres",
                min_size=1,
                max_size=5
            )
            print("Database connection pool created: ", self.db_pool)

    async def close_db(self):
        """Close database connection pool"""
        if self.db_pool:
            await self.db_pool.close()
            self.db_pool = None
    
    async def save_orderbook_message(self, raw_message, processed_orderbook):
        """Save order book message to TimescaleDB"""
        if not self.db_pool:
            await self.connect_db()
        try:
            # Extract data from the raw message
            channel = raw_message.get('channel', '')
            message_type = raw_message.get('type', '')
            data = raw_message.get('data', [{}])[0] if raw_message.get('data') else {}
            
            symbol = data.get('symbol', '')
            checksum = data.get('checksum')
            timestamp_str = data.get('timestamp')
            
            # Use message timestamp if available, otherwise current time
            # copilot did this, I think it is better to just use always current time.
            # TODO: revisit this idea
            # Sorting idx can be flipped if current time is bigger than first update timestamp
            # if timestamp_str:
            #     message_timestamp = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            # else:
            #     message_timestamp = datetime.datetime.now(datetime.timezone.utc)

            message_timestamp = datetime.datetime.now(datetime.timezone.utc)

            
            async with self.db_pool.acquire() as conn:
                # Save raw message
                await conn.execute("""
                    INSERT INTO kraken_orderbook_messages 
                    (timestamp, channel, message_type, symbol, checksum, raw_message)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, message_timestamp, channel, message_type, symbol, checksum, json.dumps(raw_message))
                
                # Save individual bid/ask entries for better querying
                if 'bids' in data:
                    for bid in data['bids']:
                        price = Decimal(str(bid['price']))
                        qty = Decimal(str(bid['qty']))
                        await conn.execute("""
                            INSERT INTO kraken_orderbook_entries 
                            (timestamp, symbol, side, price, quantity, message_type, checksum)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (timestamp, symbol, side, price) DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            message_type = EXCLUDED.message_type,
                            checksum = EXCLUDED.checksum
                        """, message_timestamp, symbol, 'bid', price, qty, message_type, checksum)
                
                if 'asks' in data:
                    for ask in data['asks']:
                        price = Decimal(str(ask['price']))
                        qty = Decimal(str(ask['qty']))
                        await conn.execute("""
                            INSERT INTO kraken_orderbook_entries 
                            (timestamp, symbol, side, price, quantity, message_type, checksum)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (timestamp, symbol, side, price) DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            message_type = EXCLUDED.message_type,
                            checksum = EXCLUDED.checksum
                        """, message_timestamp, symbol, 'ask', price, qty, message_type, checksum)
                        
        except Exception as e:
            print(f"Error saving to database: {e}")

    def handle_order_book(self, client, message):
        print("===== Order book message:", message)
        
        # Call the parent method to get the processed orderbook
        result = super().handle_order_book(client, message)
        

        asyncio.create_task(self.save_orderbook_message(message, None))


        # copilot did this, it seems the attribute never exists

        # Save to database asynchronously (fire and forget)
        # if hasattr(client, 'orderbook') and client.orderbook:
        #     # Get the processed orderbook from the client
        #     processed_orderbook = getattr(client, 'orderbook', {})
        #     # Schedule the database save
        #     asyncio.create_task(self.save_orderbook_message(message, processed_orderbook))

        
        return result

# TODO: create timestamps
async def main():
    # new Updated is ignored, I think it is a config
    # for watching trades but ignored for the orderbook
    exchange = CustomKraken({'newUpdates': True})
    try:
        # Initialize database connection
        await exchange.connect_db()
        print("Database connected successfully")
        
        for i in range(20):
            orderbook = await exchange.watch_order_book('BTC/USD')
            await sleep(1)
            # print(orderbook['asks'], orderbook['bids'])
            print(f"orderbook, idx {i}: {orderbook}")
    finally:
        await exchange.un_watch_order_book('BTC/USD')
        print("un-watch order book")
        await exchange.close_db()  # Close database connection
        await exchange.close()
        print("connection closed")


run(main())

#!/usr/bin/env python3

import asyncio
import asyncpg
import json
import datetime
from aiohttp import web, WSMsgType
import aiohttp_cors
from typing import Optional, List, Dict, Any
import argparse


class KrakenReplayServer:
    def __init__(self, db_connection_string: str, port: int = 8080):
        self.db_connection_string = db_connection_string
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        self.setup_cors()

    def setup_routes(self):
        """Setup HTTP and WebSocket routes"""
        self.app.router.add_post("/replay/prepare", self.prepare_replay)
        self.app.router.add_get("/ws", self.websocket_handler)

    def setup_cors(self):
        """Setup CORS for the application"""
        cors = aiohttp_cors.setup(self.app)

        # Configure CORS for all routes
        for route in list(self.app.router.routes()):
            cors.add(
                route,
                {
                    "*": aiohttp_cors.ResourceOptions(
                        allow_credentials=True,
                        expose_headers="*",
                        allow_headers="*",
                        allow_methods="*",
                    )
                },
            )

    async def prepare_replay(self, request):
        """HTTP POST endpoint to prepare replay from a specific date"""
        try:
            print("🎬 Preparing replay...")
            data = await request.json()
            requested_date = data.get("date")
            print(f"📅 Requested replay date: {requested_date}")

            if not requested_date:
                print("❌ ERROR: No date parameter provided")
                return web.json_response(
                    {"error": "date parameter required"}, status=400
                )

            # Parse the requested date
            try:
                if isinstance(requested_date, str):
                    # Try different date formats
                    for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
                        try:
                            requested_datetime = datetime.datetime.strptime(
                                requested_date, fmt
                            )
                            if requested_datetime.tzinfo is None:
                                requested_datetime = requested_datetime.replace(
                                    tzinfo=datetime.timezone.utc
                                )
                            break
                        except ValueError:
                            continue
                    else:
                        raise ValueError("Invalid date format")
                else:
                    requested_datetime = datetime.datetime.fromtimestamp(
                        requested_date, tz=datetime.timezone.utc
                    )
            except (ValueError, TypeError) as e:
                print(f"❌ ERROR: Date parsing failed: {e}")
                return web.json_response(
                    {"error": f"Invalid date format: {e}"}, status=400
                )

            # Find the most recent snapshot after the requested date
            print(f"🔍 Searching for snapshot after: {requested_datetime}")
            conn = await asyncpg.connect(self.db_connection_string)
            try:
                snapshot = await conn.fetchrow(
                    """
                    SELECT timestamp, raw_message
                    FROM kraken_orderbook_messages 
                    WHERE timestamp >= $1 AND message_type = 'snapshot'
                    ORDER BY timestamp ASC
                    LIMIT 1
                """,
                    requested_datetime,
                )

                if not snapshot:
                    print("❌ ERROR: No snapshot found")
                    return web.json_response(
                        {
                            "error": "No snapshot found after the requested date",
                            "requested_date": requested_datetime.isoformat(),
                        },
                        status=404,
                    )

                print(f"✅ Found snapshot at: {snapshot['timestamp']}")
                return web.json_response(
                    {
                        "status": "ready",
                        "replay_start_timestamp": snapshot["timestamp"].isoformat(),
                        "requested_date": requested_datetime.isoformat(),
                        "message": "Replay prepared. Connect via WebSocket to start.",
                    }
                )

            finally:
                await conn.close()

        except Exception as e:
            print(f"❌ ERROR: Exception in prepare_replay: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def get_replay_messages(
        self, start_timestamp: datetime.datetime, symbol: str = None
    ) -> List[Dict[str, Any]]:
        """Get all messages for replay starting from the given timestamp, optionally filtered by symbol"""
        print(f"📊 Getting replay messages from: {start_timestamp}, symbol: {symbol}")
        conn = await asyncpg.connect(self.db_connection_string)
        try:
            if symbol:
                # Filter by symbol if provided
                messages = await conn.fetch(
                    """
                    SELECT timestamp, message_type, raw_message
                    FROM kraken_orderbook_messages 
                    WHERE timestamp >= $1 AND symbol = $2
                    ORDER BY timestamp ASC
                """,
                    start_timestamp,
                    symbol,
                )
            else:
                # Original query without symbol filter
                messages = await conn.fetch(
                    """
                    SELECT timestamp, message_type, raw_message
                    FROM kraken_orderbook_messages 
                    WHERE timestamp >= $1
                    ORDER BY timestamp ASC
                """,
                    start_timestamp,
                )

            print(f"📈 Found {len(messages)} messages for replay")
            return [
                {
                    "timestamp": msg["timestamp"],
                    "type": msg["message_type"],
                    "message": json.loads(msg["raw_message"]),
                }
                for msg in messages
            ]
        except Exception as e:
            print(f"❌ ERROR: Exception in get_replay_messages: {e}")
            raise
        finally:
            await conn.close()

    async def websocket_handler(self, request):
        """WebSocket handler for replaying order book data"""
        print("🔌 WebSocket connection initiated")

        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Check for incoming WebSocket messages (subscription)
        print("📨 Waiting for subscription message...")
        subscription_data = None

        while subscription_data is None:
            try:
                msg = await ws.receive(timeout=10.0)

                if msg.type == WSMsgType.TEXT:
                    try:
                        parsed_message = json.loads(msg.data)

                        # Check if this is a subscription message
                        if parsed_message.get("method") == "subscribe":
                            subscription_data = parsed_message
                            print(f"✅ Found subscription: {subscription_data}")
                            break
                        else:
                            print(f"⏳ Non-subscription message received, waiting...")

                    except json.JSONDecodeError as e:
                        print(f"❌ Invalid JSON received: {e}")

                elif msg.type == WSMsgType.ERROR:
                    print(f"❌ WebSocket error: {msg.data}")
                    break

                elif msg.type == WSMsgType.CLOSE:
                    print("🔌 WebSocket closed")
                    break

                else:
                    print(f"❓ Non-text message type: {msg.type}")

            except asyncio.TimeoutError:
                print("⏱️ Timeout waiting for message, continuing...")
                continue

            except Exception as e:
                print(f"❌ Error receiving message: {e}")
                break

        if subscription_data is None:
            print("❌ ERROR: No valid subscription message received")
            await ws.send_str(json.dumps({"error": "No subscription message received"}))
            return ws

        # Extract symbol from subscription
        try:
            params = subscription_data.get("params", {})
            symbols = params.get("symbol", [])
            if not symbols:
                print("❌ ERROR: No symbols in subscription")
                await ws.send_str(json.dumps({"error": "No symbols in subscription"}))
                return ws

            # Assume single symbol for now
            subscribed_symbol = symbols[0] if isinstance(symbols, list) else symbols
            print(f"💰 Subscribed symbol: {subscribed_symbol}")

        except Exception as e:
            print(f"❌ ERROR: Failed to extract symbol: {e}")
            await ws.send_str(json.dumps({"error": "Invalid subscription format"}))
            return ws

        # Get replay start date from query parameters
        start_date_param = request.query.get("start_date")
        if not start_date_param:
            print("❌ ERROR: No start_date parameter")
            await ws.send_str(
                json.dumps({"error": "start_date parameter required in query string"})
            )
            return ws

        try:
            # Parse start date
            start_date_param = start_date_param.replace(" ", "+")
            start_datetime = datetime.datetime.fromisoformat(
                start_date_param.replace("Z", "+00:00")
            )
            print(f"📅 Starting replay from: {start_datetime}")
        except ValueError:
            print("❌ ERROR: Invalid start_date format")
            await ws.send_str(
                json.dumps(
                    {
                        "error": "Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)"
                    }
                )
            )
            return ws

        try:
            # Get all messages for replay, filtered by subscribed symbol
            messages = await self.get_replay_messages(start_datetime, subscribed_symbol)

            if not messages:
                print(f"❌ ERROR: No messages found for symbol: {subscribed_symbol}")
                await ws.send_str(
                    json.dumps(
                        {
                            "error": f"No messages found for replay from the specified date for symbol {subscribed_symbol}"
                        }
                    )
                )
                return ws

            print(f"🎬 Starting replay with {len(messages)} messages for {subscribed_symbol}")

            # Send connection acknowledgment (similar to Kraken)
            await ws.send_str(
                json.dumps(
                    {
                        "method": "subscribe",
                        "result": {
                            "channel": "book",
                            "snapshot": True,
                            "symbol": subscribed_symbol,
                        },
                        "success": True,
                        "time_in": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                        "time_out": datetime.datetime.now(
                            datetime.timezone.utc
                        ).isoformat(),
                    }
                )
            )

            # Replay messages
            last_was_snapshot = False
            previous_timestamp = None
            sent_count = 0

            for i, msg_data in enumerate(messages):
                message = msg_data["message"]
                msg_type = msg_data["type"]
                current_timestamp = msg_data["timestamp"]

                # If we encounter a snapshot after starting, and we've been sending updates, stop
                if msg_type == "snapshot" and last_was_snapshot and i > 0:
                    print("🔄 Encountered new snapshot, ending replay")
                    break

                # Calculate real delay based on timestamp difference
                if previous_timestamp is not None:
                    delay = (current_timestamp - previous_timestamp).total_seconds()
                    print(f"⏱️ Waiting {delay:.2f}s (real timing)")
                    await asyncio.sleep(delay)

                # Send the message in Kraken WebSocket format
                await ws.send_str(json.dumps(message))
                sent_count += 1
                
                # Log progress every 100 messages
                if sent_count % 100 == 0:
                    print(f"📤 Sent {sent_count}/{len(messages)} messages")

                last_was_snapshot = msg_type == "snapshot"
                previous_timestamp = current_timestamp

                # Check if client disconnected
                if ws.closed:
                    print("🔌 Client disconnected")
                    break

            print(f"✅ Replay completed: {sent_count} messages sent")

        except Exception as e:
            print(f"❌ ERROR during replay: {e}")
            await ws.send_str(json.dumps({"error": str(e)}))

        return ws

    async def start_server(self):
        """Start the replay server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()
        print(f"🚀 Kraken Replay Server started on http://0.0.0.0:{self.port}")
        print(f"📊 WebSocket endpoint: ws://localhost:{self.port}/ws")
        print(f"📋 Prepare replay: POST http://localhost:{self.port}/replay/prepare")
        return runner


async def main():
    parser = argparse.ArgumentParser(description="Kraken Order Book Replay Server")
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=8080,
        help="Port to run the server on (default: 8080)",
    )
    parser.add_argument(
        "--db",
        default="postgresql://postgres:password@localhost/postgres",
        help="Database connection string",
    )

    args = parser.parse_args()

    server = KrakenReplayServer(args.db, args.port)
    runner = await server.start_server()

    try:
        # Keep the server running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

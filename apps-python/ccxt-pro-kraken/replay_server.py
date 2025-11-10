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
        self.app.router.add_post('/replay/prepare', self.prepare_replay)
        self.app.router.add_get('/ws', self.websocket_handler)
        
    def setup_cors(self):
        """Setup CORS for the application"""
        cors = aiohttp_cors.setup(self.app)
        
        # Configure CORS for all routes
        for route in list(self.app.router.routes()):
            cors.add(route, {
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*",
                    allow_methods="*"
                )
            })
    
    async def prepare_replay(self, request):
        """HTTP POST endpoint to prepare replay from a specific date"""
        try:
            print("===== 0 Preparing replay...")
            data = await request.json()
            requested_date = data.get('date')

            print(f"===== 1 Requested date for replay: {requested_date}")
            
            if not requested_date:
                print("===== 2 ERROR: No date parameter provided")
                return web.json_response({'error': 'date parameter required'}, status=400)
            
            # Parse the requested date
            print("===== 3 Starting date parsing...")
            try:
                if isinstance(requested_date, str):
                    print(f"===== 4 Parsing string date: {requested_date}")
                    # Try different date formats
                    for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ']:
                        try:
                            print(f"===== 5 Trying format: {fmt}")
                            requested_datetime = datetime.datetime.strptime(requested_date, fmt)
                            if requested_datetime.tzinfo is None:
                                requested_datetime = requested_datetime.replace(tzinfo=datetime.timezone.utc)
                            print(f"===== 6 Successfully parsed date: {requested_datetime}")
                            break
                        except ValueError:
                            print(f"===== 7 Format {fmt} failed, trying next...")
                            continue
                    else:
                        print("===== 8 ERROR: All date formats failed")
                        raise ValueError("Invalid date format")
                else:
                    print(f"===== 9 Parsing timestamp: {requested_date}")
                    requested_datetime = datetime.datetime.fromtimestamp(requested_date, tz=datetime.timezone.utc)
                    print(f"===== 10 Successfully parsed timestamp: {requested_datetime}")
            except (ValueError, TypeError) as e:
                print(f"===== 11 ERROR: Date parsing failed: {e}")
                return web.json_response({'error': f'Invalid date format: {e}'}, status=400)
            
            # Find the most recent snapshot after the requested date
            print(f"===== 12 Connecting to database: {self.db_connection_string}")
            conn = await asyncpg.connect(self.db_connection_string)
            try:
                print(f"===== 13 Querying for snapshot after: {requested_datetime}")
                snapshot = await conn.fetchrow("""
                    SELECT timestamp, raw_message
                    FROM kraken_orderbook_messages 
                    WHERE timestamp >= $1 AND message_type = 'snapshot'
                    ORDER BY timestamp ASC
                    LIMIT 1
                """, requested_datetime)
                
                print(f"===== 14 Query result: {snapshot}")
                if not snapshot:
                    print("===== 15 ERROR: No snapshot found")
                    return web.json_response({
                        'error': 'No snapshot found after the requested date',
                        'requested_date': requested_datetime.isoformat()
                    }, status=404)
                
                print(f"===== 16 Found snapshot at: {snapshot['timestamp']}")
                return web.json_response({
                    'status': 'ready',
                    'replay_start_timestamp': snapshot['timestamp'].isoformat(),
                    'requested_date': requested_datetime.isoformat(),
                    'message': 'Replay prepared. Connect via WebSocket to start.'
                })
                
            finally:
                print("===== 17 Closing database connection")
                await conn.close()
                
        except Exception as e:
            print(f"===== 18 ERROR: Exception in prepare_replay: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_replay_messages(self, start_timestamp: datetime.datetime) -> List[Dict[str, Any]]:
        """Get all messages for replay starting from the given timestamp"""
        print(f"===== 19 Getting replay messages from: {start_timestamp}")
        conn = await asyncpg.connect(self.db_connection_string)
        try:
            print("===== 20 Executing messages query...")
            messages = await conn.fetch("""
                SELECT timestamp, message_type, raw_message
                FROM kraken_orderbook_messages 
                WHERE timestamp >= $1
                ORDER BY timestamp ASC
            """, start_timestamp)
            
            print(f"===== 21 Found {len(messages)} messages")
            return [
                {
                    'timestamp': msg['timestamp'],
                    'type': msg['message_type'],
                    'message': json.loads(msg['raw_message'])
                } 
                for msg in messages
            ]
        except Exception as e:
            print(f"===== 22 ERROR: Exception in get_replay_messages: {e}")
            raise
        finally:
            print("===== 23 Closing messages query connection")
            await conn.close()
    
    async def websocket_handler(self, request):
        """WebSocket handler for replaying order book data"""
        print("===== 24 WebSocket connection initiated")
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        print("===== 25 WebSocket prepared")
        
        # Get replay start date from query parameters
        start_date_param = request.query.get('start_date')
        print(f"===== 26 WebSocket start_date param: {start_date_param}")
        if not start_date_param:
            print("===== 27 ERROR: No start_date parameter")
            await ws.send_str(json.dumps({
                'error': 'start_date parameter required in query string'
            }))
            return ws
        
        try:
            print("===== 28 Parsing WebSocket start date...", start_date_param)
            # Parse start date
            # start_datetime = datetime.datetime.fromisoformat(start_date_param.replace('Z', '+00:00'))
            # TODO: fix datetimes mess
            start_date_param = start_date_param.replace(' ', '+')
            print(f"===== 28-1 Modified start_date_param: {start_date_param}")
            start_datetime = datetime.datetime.fromisoformat(start_date_param.replace('Z', '+00:00'))
            print(f"===== 29 Parsed WebSocket start date: {start_datetime}")
        except ValueError:
            print("===== 30 ERROR: Invalid start_date format in WebSocket")
            await ws.send_str(json.dumps({
                'error': 'Invalid start_date format. Use ISO format (YYYY-MM-DDTHH:MM:SS)'
            }))
            return ws
        
        print(f"===== 31 Starting replay from {start_datetime}")
        
        try:
            print("===== 32 Getting messages for replay...")
            # Get all messages for replay
            messages = await self.get_replay_messages(start_datetime)
            
            if not messages:
                print("===== 33 ERROR: No messages found for WebSocket replay")
                await ws.send_str(json.dumps({
                    'error': 'No messages found for replay from the specified date'
                }))
                return ws
            
            print(f"===== 34 Found {len(messages)} messages for replay")
            
            # Send connection acknowledgment (similar to Kraken)
            print("===== 35 Sending connection acknowledgment...")
            await ws.send_str(json.dumps({
                "method": "subscribe",
                "result": {
                    "channel": "book", 
                    "snapshot": True,
                    "symbol": "BTC/USD"
                },
                "success": True,
                "time_in": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "time_out": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }))
            print("===== 36 Connection acknowledgment sent")
            
            # Replay messages
            print("===== 37 Starting message replay loop...")
            last_was_snapshot = False
            previous_timestamp = None
            
            for i, msg_data in enumerate(messages):
                print(f"===== 38 Processing message {i+1}/{len(messages)}")
                message = msg_data['message']
                msg_type = msg_data['type']
                current_timestamp = msg_data['timestamp']
                
                # If we encounter a snapshot after starting, and we've been sending updates, stop
                if msg_type == 'snapshot' and last_was_snapshot and i > 0:
                    print("===== 39 Encountered new snapshot, ending replay")
                    break
                
                # Calculate real delay based on timestamp difference
                if previous_timestamp is not None:
                    time_diff = (current_timestamp - previous_timestamp).total_seconds()
                    # Cap the delay to a reasonable maximum (e.g., 60 seconds)
                    delay = min(time_diff, 60.0)
                    if delay > 0:
                        print(f"===== 40 ⏱️  Waiting {delay:.3f}s (real delay between messages)")
                        await asyncio.sleep(delay)
                
                # Send the message in Kraken WebSocket format
                print(f"===== 41 Sending message {i+1}")
                await ws.send_str(json.dumps(message))
                print(f"===== 42 📤 Sent message {i+1}/{len(messages)} at {current_timestamp}")
                
                last_was_snapshot = (msg_type == 'snapshot')
                previous_timestamp = current_timestamp
                
                # Check if client disconnected
                if ws.closed:
                    print("===== 43 Client disconnected, breaking loop")
                    break
            
            print("===== 44 Replay completed")
            
        except Exception as e:
            print(f"===== 45 ERROR: Exception during WebSocket replay: {e}")
            await ws.send_str(json.dumps({'error': str(e)}))
        
        print("===== 46 WebSocket handler returning")
        return ws

    async def start_server(self):
        """Start the replay server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        print(f"🚀 Kraken Replay Server started on http://0.0.0.0:{self.port}")
        print(f"📊 WebSocket endpoint: ws://localhost:{self.port}/ws")
        print(f"📋 Prepare replay: POST http://localhost:{self.port}/replay/prepare")
        return runner

async def main():
    parser = argparse.ArgumentParser(description='Kraken Order Book Replay Server')
    parser.add_argument('--port', '-p', type=int, default=8080, 
                        help='Port to run the server on (default: 8080)')
    parser.add_argument('--db', default="postgresql://postgres:password@localhost/postgres",
                        help='Database connection string')
    
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

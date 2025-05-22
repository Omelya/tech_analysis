import asyncio
import json
import time
from typing import Dict, Set, Optional, Any, List
from datetime import datetime
import websockets
from websockets.legacy.server import WebSocketServerProtocol
import structlog

from ..config import settings
from ..utils.metrics_collector import metrics_collector
from .data_processor import data_processor


class WebSocketServer:
    """WebSocket server for real-time data streaming to Laravel"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="websocket_server")
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # client_id -> set of subscription keys
        self.reverse_subscriptions: Dict[str, Set[str]] = {}  # subscription_key -> set of client_ids

        # Buffer for data aggregation
        self.data_buffers: Dict[str, Dict[str, Any]] = {}
        self.buffer_timers: Dict[str, asyncio.Task] = {}

        # Server instance
        self.server = None
        self.running = False

        # Stream management
        self.stream_handlers = {
            'ticker': self._handle_ticker_stream,
            'orderbook': self._handle_orderbook_stream,
            'klines': self._handle_klines_stream,
            'trades': self._handle_trades_stream
        }

    async def start_server(self, host: str = "0.0.0.0", port: int = 8001):
        """Start WebSocket server"""
        try:
            self.server = await websockets.serve(
                self.handle_client,
                host,
                port,
                ping_interval=settings.websocket_ping_interval,
                ping_timeout=settings.websocket_ping_timeout,
                max_size=1024 * 1024,  # 1MB max message size
                compression=None  # Disable compression for performance
            )

            self.running = True
            self.logger.info("WebSocket server started", host=host, port=port)

            # Start data streaming tasks
            await self._start_data_streams()

        except Exception as e:
            self.logger.error("Failed to start WebSocket server", error=str(e))
            raise

    async def stop_server(self):
        """Stop WebSocket server"""
        try:
            self.running = False

            # Cancel buffer timers
            for timer in self.buffer_timers.values():
                timer.cancel()
            self.buffer_timers.clear()

            # Close all client connections
            if self.clients:
                await asyncio.gather(
                    *[client.close() for client in self.clients.values()],
                    return_exceptions=True
                )

            # Stop server
            if self.server:
                self.server.close()
                await self.server.wait_closed()

            self.logger.info("WebSocket server stopped")

        except Exception as e:
            self.logger.error("Error stopping WebSocket server", error=str(e))

    async def handle_client(self, websocket: WebSocketServerProtocol, path: str):
        """Handle new WebSocket client connection"""
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}:{id(websocket)}"

        try:
            self.clients[client_id] = websocket
            self.subscriptions[client_id] = set()

            self.logger.info("Client connected", client_id=client_id, path=path)
            metrics_collector.increment_counter('websocket_connections', {'status': 'connected'})

            # Send welcome message
            await self._send_to_client(client_id, {
                'type': 'connection',
                'status': 'connected',
                'client_id': client_id,
                'timestamp': datetime.now().isoformat()
            })

            # Handle client messages
            async for message in websocket:
                await self._handle_client_message(client_id, message)

        except websockets.exceptions.ConnectionClosed:
            self.logger.info("Client disconnected", client_id=client_id)
        except Exception as e:
            self.logger.error("Error handling client", client_id=client_id, error=str(e))
        finally:
            await self._cleanup_client(client_id)

    async def _handle_client_message(self, client_id: str, message: str):
        """Handle message from WebSocket client"""
        try:
            data = json.loads(message)
            message_type = data.get('type')

            if message_type == 'subscribe':
                await self._handle_subscribe(client_id, data)
            elif message_type == 'unsubscribe':
                await self._handle_unsubscribe(client_id, data)
            elif message_type == 'ping':
                await self._handle_ping(client_id)
            else:
                await self._send_error(client_id, f"Unknown message type: {message_type}")

        except json.JSONDecodeError:
            await self._send_error(client_id, "Invalid JSON message")
        except Exception as e:
            self.logger.error("Error handling client message",
                              client_id=client_id, error=str(e))
            await self._send_error(client_id, "Internal server error")

    async def _handle_subscribe(self, client_id: str, data: Dict[str, Any]):
        """Handle subscription request"""
        try:
            stream_type = data.get('stream')
            exchange = data.get('exchange')
            symbol = data.get('symbol')
            timeframe = data.get('timeframe')  # Optional, for klines

            if not all([stream_type, exchange, symbol]):
                await self._send_error(client_id, "Missing required fields: stream, exchange, symbol")
                return

            # Build subscription key
            if stream_type == 'klines' and timeframe:
                subscription_key = f"{stream_type}:{exchange}:{symbol}:{timeframe}"
            else:
                subscription_key = f"{stream_type}:{exchange}:{symbol}"

            # Add subscription
            self.subscriptions[client_id].add(subscription_key)

            if subscription_key not in self.reverse_subscriptions:
                self.reverse_subscriptions[subscription_key] = set()
            self.reverse_subscriptions[subscription_key].add(client_id)

            # Send confirmation
            await self._send_to_client(client_id, {
                'type': 'subscription',
                'status': 'subscribed',
                'subscription': subscription_key,
                'timestamp': datetime.now().isoformat()
            })

            self.logger.info("Client subscribed",
                             client_id=client_id, subscription=subscription_key)
            metrics_collector.increment_counter('websocket_subscriptions',
                                                {'stream': stream_type, 'exchange': exchange})

        except Exception as e:
            self.logger.error("Error handling subscribe", client_id=client_id, error=str(e))
            await self._send_error(client_id, "Failed to subscribe")

    async def _handle_unsubscribe(self, client_id: str, data: Dict[str, Any]):
        """Handle unsubscription request"""
        try:
            subscription_key = data.get('subscription')

            if not subscription_key:
                await self._send_error(client_id, "Missing subscription key")
                return

            # Remove subscription
            if client_id in self.subscriptions:
                self.subscriptions[client_id].discard(subscription_key)

            if subscription_key in self.reverse_subscriptions:
                self.reverse_subscriptions[subscription_key].discard(client_id)

                # Clean up empty subscription
                if not self.reverse_subscriptions[subscription_key]:
                    del self.reverse_subscriptions[subscription_key]

            # Send confirmation
            await self._send_to_client(client_id, {
                'type': 'subscription',
                'status': 'unsubscribed',
                'subscription': subscription_key,
                'timestamp': datetime.now().isoformat()
            })

            self.logger.info("Client unsubscribed",
                             client_id=client_id, subscription=subscription_key)

        except Exception as e:
            self.logger.error("Error handling unsubscribe", client_id=client_id, error=str(e))
            await self._send_error(client_id, "Failed to unsubscribe")

    async def _handle_ping(self, client_id: str):
        """Handle ping message"""
        await self._send_to_client(client_id, {
            'type': 'pong',
            'timestamp': datetime.now().isoformat()
        })

    async def _send_to_client(self, client_id: str, data: Dict[str, Any]):
        """Send data to specific client"""
        if client_id not in self.clients:
            return

        try:
            websocket = self.clients[client_id]
            message = json.dumps(data)
            await websocket.send(message)

        except websockets.exceptions.ConnectionClosed:
            await self._cleanup_client(client_id)
        except Exception as e:
            self.logger.error("Error sending to client", client_id=client_id, error=str(e))
            await self._cleanup_client(client_id)

    async def _send_error(self, client_id: str, error_message: str):
        """Send error message to client"""
        await self._send_to_client(client_id, {
            'type': 'error',
            'message': error_message,
            'timestamp': datetime.now().isoformat()
        })

    async def _cleanup_client(self, client_id: str):
        """Clean up client data"""
        # Remove from clients
        if client_id in self.clients:
            del self.clients[client_id]

        # Remove subscriptions
        if client_id in self.subscriptions:
            for subscription_key in self.subscriptions[client_id]:
                if subscription_key in self.reverse_subscriptions:
                    self.reverse_subscriptions[subscription_key].discard(client_id)

                    # Clean up empty subscriptions
                    if not self.reverse_subscriptions[subscription_key]:
                        del self.reverse_subscriptions[subscription_key]

            del self.subscriptions[client_id]

        metrics_collector.increment_counter('websocket_connections', {'status': 'disconnected'})

    async def _start_data_streams(self):
        """Start background tasks for data streaming"""
        # Start buffer flush task
        asyncio.create_task(self._buffer_flush_loop())

        self.logger.info("Data streaming tasks started")

    async def _buffer_flush_loop(self):
        """Periodically flush buffered data"""
        while self.running:
            try:
                await asyncio.sleep(0.1)  # Flush every 100ms
                await self._flush_all_buffers()

            except Exception as e:
                self.logger.error("Error in buffer flush loop", error=str(e))

    async def _flush_all_buffers(self):
        """Flush all data buffers"""
        current_time = time.time()

        for subscription_key, buffer_data in list(self.data_buffers.items()):
            # Check if buffer should be flushed (age > 100ms or size > threshold)
            buffer_age = current_time - buffer_data.get('timestamp', 0)

            if buffer_age > 0.1 or len(buffer_data.get('data', [])) > 10:
                await self._flush_buffer(subscription_key)

    async def _flush_buffer(self, subscription_key: str):
        """Flush specific buffer"""
        if subscription_key not in self.data_buffers:
            return

        buffer_data = self.data_buffers.pop(subscription_key)

        if subscription_key in self.reverse_subscriptions:
            clients = self.reverse_subscriptions[subscription_key].copy()

            # Send to all subscribed clients
            tasks = []
            for client_id in clients:
                tasks.append(self._send_to_client(client_id, buffer_data))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    # Stream-specific handlers
    async def _handle_ticker_stream(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Handle ticker stream data"""
        subscription_key = f"ticker:{exchange}:{symbol}"

        stream_data = {
            'type': 'ticker',
            'exchange': exchange,
            'symbol': symbol,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._buffer_data(subscription_key, stream_data)

    async def _handle_orderbook_stream(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Handle orderbook stream data"""
        subscription_key = f"orderbook:{exchange}:{symbol}"

        stream_data = {
            'type': 'orderbook',
            'exchange': exchange,
            'symbol': symbol,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._buffer_data(subscription_key, stream_data)

    async def _handle_klines_stream(self, exchange: str, symbol: str, timeframe: str, data: Dict[str, Any]):
        """Handle klines stream data"""
        subscription_key = f"klines:{exchange}:{symbol}:{timeframe}"

        stream_data = {
            'type': 'klines',
            'exchange': exchange,
            'symbol': symbol,
            'timeframe': timeframe,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._buffer_data(subscription_key, stream_data)

    async def _handle_trades_stream(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Handle trades stream data"""
        subscription_key = f"trades:{exchange}:{symbol}"

        stream_data = {
            'type': 'trades',
            'exchange': exchange,
            'symbol': symbol,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._buffer_data(subscription_key, stream_data)

    async def _buffer_data(self, subscription_key: str, data: Dict[str, Any]):
        """Buffer data for aggregation"""
        if subscription_key not in self.reverse_subscriptions:
            return  # No subscribers

        # For real-time critical data, send immediately
        if data['type'] in ['ticker', 'trades']:
            clients = self.reverse_subscriptions[subscription_key].copy()
            tasks = []

            for client_id in clients:
                tasks.append(self._send_to_client(client_id, data))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # Buffer other data types
            if subscription_key not in self.data_buffers:
                self.data_buffers[subscription_key] = {
                    'data': [],
                    'timestamp': time.time()
                }

            self.data_buffers[subscription_key]['data'].append(data)

    def get_server_stats(self) -> Dict[str, Any]:
        """Get WebSocket server statistics"""
        return {
            'clients_connected': len(self.clients),
            'total_subscriptions': sum(len(subs) for subs in self.subscriptions.values()),
            'unique_subscriptions': len(self.reverse_subscriptions),
            'running': self.running,
            'server_started': self.server is not None,
            'buffered_data_count': sum(len(buf.get('data', [])) for buf in self.data_buffers.values())
        }


# Global instance
websocket_server = WebSocketServer()

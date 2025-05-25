import asyncio
import json
import time
from typing import Dict, Set, Optional, Any, List, Callable
from datetime import datetime
import websockets
from websockets.legacy.server import WebSocketServerProtocol
import structlog

from ..config import settings
from ..stream_processing.monitoring_system import monitoring_system


class WebSocketServer:
    """WebSocket server integrated with stream processing system"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="websocket_server")
        self.clients: Dict[str, WebSocketServerProtocol] = {}
        self.subscriptions: Dict[str, Set[str]] = {}  # client_id -> set of subscription keys
        self.reverse_subscriptions: Dict[str, Set[str]] = {}  # subscription_key -> set of client_ids

        # Server instance
        self.server = None
        self.running = False

        # Message routing to stream processor
        self.message_router: Optional[Callable] = None

        # Stream data cache for clients
        self.stream_cache: Dict[str, Any] = {}

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

        except Exception as e:
            self.logger.error("Failed to start WebSocket server", error=str(e))
            raise

    async def stop_server(self):
        """Stop WebSocket server"""
        try:
            self.running = False

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
            monitoring_system.increment_counter(metric_name='websocket_connections', tags={'status': 'connected'})

            # Send welcome message
            await self._send_to_client(client_id, {
                'type': 'connection',
                'status': 'connected',
                'client_id': client_id,
                'timestamp': datetime.now().isoformat(),
                'features': ['stream_processing', 'real_time_data', 'smart_buffering']
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
            elif message_type == 'get_stats':
                await self._handle_get_stats(client_id)
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

            # Send cached data if available
            if subscription_key in self.stream_cache:
                await self._send_to_client(client_id, {
                    'type': 'data',
                    'subscription': subscription_key,
                    'data': self.stream_cache[subscription_key],
                    'cached': True
                })

            self.logger.info("Client subscribed",
                             client_id=client_id, subscription=subscription_key)
            monitoring_system.increment_counter('websocket_subscriptions',
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
            'timestamp': datetime.now().isoformat(),
            'server_time': time.time()
        })

    async def _handle_get_stats(self, client_id: str):
        """Handle stats request"""
        try:
            stats = self.get_server_stats()
            system_stats = monitoring_system.get_comprehensive_status()

            await self._send_to_client(client_id, {
                'type': 'stats',
                'websocket_stats': stats,
                'system_stats': system_stats,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            self.logger.error("Error getting stats", client_id=client_id, error=str(e))
            await self._send_error(client_id, "Failed to get stats")

    async def _send_to_client(self, client_id: str, data: Dict[str, Any]):
        """Send data to specific client"""
        if client_id not in self.clients:
            return

        try:
            websocket = self.clients[client_id]
            message = json.dumps(data, default=str)
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

        monitoring_system.increment_counter(metric_name='websocket_connections', tags={'status': 'disconnected'})

    # Stream data handling methods - these are called by stream processing system
    async def broadcast_ticker_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Broadcast ticker data to subscribed clients"""
        subscription_key = f"ticker:{exchange}:{symbol}"

        # Cache data
        self.stream_cache[subscription_key] = {
            'type': 'ticker',
            'exchange': exchange,
            'symbol': symbol,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._broadcast_to_subscribers(subscription_key, self.stream_cache[subscription_key])

    async def broadcast_orderbook_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Broadcast orderbook data to subscribed clients"""
        subscription_key = f"orderbook:{exchange}:{symbol}"

        # Cache data (limited to prevent memory bloat)
        cached_data = {
            'type': 'orderbook',
            'exchange': exchange,
            'symbol': symbol,
            'data': {
                'bids': data.get('bids', [])[:10],  # Top 10 levels only
                'asks': data.get('asks', [])[:10],
                'timestamp': data.get('timestamp'),
                'best_bid': data.get('bids', [[0]])[0][0] if data.get('bids') else 0,
                'best_ask': data.get('asks', [[0]])[0][0] if data.get('asks') else 0
            },
            'timestamp': datetime.now().isoformat()
        }

        self.stream_cache[subscription_key] = cached_data
        await self._broadcast_to_subscribers(subscription_key, cached_data)

    async def broadcast_klines_data(self, exchange: str, symbol: str, timeframe: str, data: Dict[str, Any]):
        """Broadcast klines data to subscribed clients"""
        subscription_key = f"klines:{exchange}:{symbol}:{timeframe}"

        # Cache data
        self.stream_cache[subscription_key] = {
            'type': 'klines',
            'exchange': exchange,
            'symbol': symbol,
            'timeframe': timeframe,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }

        await self._broadcast_to_subscribers(subscription_key, self.stream_cache[subscription_key])

    async def broadcast_trades_data(self, exchange: str, symbol: str, data: List[Dict[str, Any]]):
        """Broadcast trades data to subscribed clients"""
        subscription_key = f"trades:{exchange}:{symbol}"

        # Don't cache trades (too volatile)
        trade_data = {
            'type': 'trades',
            'exchange': exchange,
            'symbol': symbol,
            'data': data[-10:] if len(data) > 10 else data,  # Last 10 trades only
            'timestamp': datetime.now().isoformat()
        }

        await self._broadcast_to_subscribers(subscription_key, trade_data)

    async def _broadcast_to_subscribers(self, subscription_key: str, data: Dict[str, Any]):
        """Broadcast data to all subscribers of a key"""
        if subscription_key not in self.reverse_subscriptions:
            return

        clients = self.reverse_subscriptions[subscription_key].copy()

        # Send to all subscribed clients
        tasks = []
        for client_id in clients:
            message_data = {
                'type': 'stream_data',
                'subscription': subscription_key,
                **data
            }
            tasks.append(self._send_to_client(client_id, message_data))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_server_stats(self) -> Dict[str, Any]:
        """Get WebSocket server statistics"""
        return {
            'clients_connected': len(self.clients),
            'total_subscriptions': sum(len(subs) for subs in self.subscriptions.values()),
            'unique_subscriptions': len(self.reverse_subscriptions),
            'running': self.running,
            'server_started': self.server is not None,
            'cached_streams': len(self.stream_cache),
            'subscription_breakdown': self._get_subscription_breakdown()
        }

    def _get_subscription_breakdown(self) -> Dict[str, int]:
        """Get breakdown of subscriptions by type"""
        breakdown = {}
        for subscription_key in self.reverse_subscriptions:
            stream_type = subscription_key.split(':')[0]
            breakdown[stream_type] = breakdown.get(stream_type, 0) + len(self.reverse_subscriptions[subscription_key])
        return breakdown

    # Integration methods for stream processing
    async def handle_stream_processor_data(self, exchange: str, symbol: str,
                                           message_type: str, data: Dict[str, Any]):
        """Handle data from stream processor"""
        try:
            if message_type == 'ticker':
                await self.broadcast_ticker_data(exchange, symbol, data)
            elif message_type == 'orderbook':
                await self.broadcast_orderbook_data(exchange, symbol, data)
            elif message_type == 'klines':
                timeframe = data.get('timeframe', '1m')
                await self.broadcast_klines_data(exchange, symbol, timeframe, data)
            elif message_type == 'trades':
                trades = data if isinstance(data, list) else [data]
                await self.broadcast_trades_data(exchange, symbol, trades)

        except Exception as e:
            self.logger.error("Error handling stream processor data",
                              exchange=exchange, symbol=symbol,
                              message_type=message_type, error=str(e))


# Global instance
websocket_server = WebSocketServer()

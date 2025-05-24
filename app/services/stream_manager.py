import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict, deque
import structlog

from ..exchanges.manager import exchange_manager
from ..config import settings
from .websocket_server import websocket_server
from ..utils.rabbitmq import RabbitMQPublisher


class StreamManager:
    """Manages real-time data streams with buffering and aggregation"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="stream_manager")

        # Active streams
        self.active_streams: Dict[str, asyncio.Task] = {}
        self.stream_subscriptions: Dict[str, List[Callable]] = defaultdict(list)

        # Buffering system
        self.buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.buffer_timers: Dict[str, float] = {}
        self.buffer_config = {
            'ticker': {'max_size': 5, 'max_age': 0.5},  # 500ms or 5 items
            'orderbook': {'max_size': 3, 'max_age': 0.2},  # 200ms or 3 items
            'klines': {'max_size': 1, 'max_age': 1.0},  # 1s or 1 item
            'trades': {'max_size': 10, 'max_age': 0.1}  # 100ms or 10 items
        }

        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None
        self.running = False

    async def initialize(self):
        """Initialize stream manager"""
        self.rabbitmq_publisher = RabbitMQPublisher(settings.rabbitmq_url)
        await self.rabbitmq_publisher.initialize()

        self.running = True

        # Start buffer flush task
        asyncio.create_task(self._buffer_flush_loop())

        self.logger.info("Stream manager initialized")

    async def start_stream(self, exchange: str, symbol: str, stream_type: str,
                           timeframe: Optional[str] = None) -> str:
        """Start a new data stream"""
        stream_key = f"{stream_type}:{exchange}:{symbol}"
        if timeframe:
            stream_key += f":{timeframe}"

        if stream_key in self.active_streams:
            return stream_key

        # Create stream task
        task = asyncio.create_task(
            self._stream_worker(exchange, symbol, stream_type, timeframe)
        )
        self.active_streams[stream_key] = task

        self.logger.info("Stream started", stream=stream_key)
        return stream_key

    async def stop_stream(self, stream_key: str):
        """Stop a data stream"""
        if stream_key in self.active_streams:
            self.active_streams[stream_key].cancel()
            del self.active_streams[stream_key]
            self.logger.info("Stream stopped", stream=stream_key)

    async def subscribe_to_stream(self, stream_key: str, callback: Callable):
        """Subscribe callback to stream"""
        self.stream_subscriptions[stream_key].append(callback)

    async def _stream_worker(self, exchange: str, symbol: str, stream_type: str,
                             timeframe: Optional[str] = None):
        """Worker for handling individual stream"""
        try:
            adapter = await exchange_manager.get_public_adapter(exchange)
            if not adapter:
                return

            async def data_callback(data):
                await self._handle_stream_data(exchange, symbol, stream_type, data, timeframe)

            # Subscribe based on stream type
            if stream_type == 'ticker':
                await adapter.websocket.subscribe_ticker(symbol, data_callback)
            elif stream_type == 'orderbook':
                await adapter.websocket.subscribe_orderbook(symbol, data_callback)
            elif stream_type == 'klines' and timeframe:
                await adapter.websocket.subscribe_klines(symbol, timeframe, data_callback)

        except Exception as e:
            self.logger.error("Stream worker error",
                              exchange=exchange, symbol=symbol, stream_type=stream_type, error=str(e))

    async def _handle_stream_data(self, exchange: str, symbol: str, stream_type: str,
                                  data: Dict[str, Any], timeframe: Optional[str] = None):
        """Handle incoming stream data with buffering"""
        stream_key = f"{stream_type}:{exchange}:{symbol}"
        if timeframe:
            stream_key += f":{timeframe}"

        # Add to buffer
        self.buffers[stream_key].append({
            'data': data,
            'timestamp': time.time(),
            'exchange': exchange,
            'symbol': symbol,
            'type': stream_type,
            'timeframe': timeframe
        })

        # Mark buffer for potential flush
        if stream_key not in self.buffer_timers:
            self.buffer_timers[stream_key] = time.time()

        # Check if immediate flush needed
        config = self.buffer_config.get(stream_type, {'max_size': 1, 'max_age': 1.0})

        if (len(self.buffers[stream_key]) >= config['max_size'] or
                time.time() - self.buffer_timers[stream_key] >= config['max_age']):
            await self._flush_buffer(stream_key)

    async def _buffer_flush_loop(self):
        """Periodically flush aged buffers"""
        while self.running:
            try:
                current_time = time.time()

                for stream_key, start_time in list(self.buffer_timers.items()):
                    stream_type = stream_key.split(':')[0]
                    config = self.buffer_config.get(stream_type, {'max_age': 1.0})

                    if current_time - start_time >= config['max_age']:
                        await self._flush_buffer(stream_key)

                await asyncio.sleep(0.05)  # Check every 50ms

            except Exception as e:
                self.logger.error("Buffer flush loop error", error=str(e))

    async def _flush_buffer(self, stream_key: str):
        """Flush buffer and distribute data"""
        if stream_key not in self.buffers or not self.buffers[stream_key]:
            return

        # Get buffered data
        buffer_data = list(self.buffers[stream_key])
        self.buffers[stream_key].clear()

        if stream_key in self.buffer_timers:
            del self.buffer_timers[stream_key]

        # Aggregate data if multiple items
        aggregated_data = self._aggregate_buffer_data(buffer_data)

        # Distribute to subscribers
        await self._distribute_data(stream_key, aggregated_data)

    def _aggregate_buffer_data(self, buffer_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate buffered data intelligently"""
        if not buffer_data:
            return {}

        if len(buffer_data) == 1:
            return buffer_data[0]

        # For multiple items, take the most recent for most data types
        latest = buffer_data[-1]
        stream_type = latest['type']

        if stream_type == 'ticker':
            # For tickers, use latest price but aggregate volume
            result = latest.copy()
            total_volume = sum(item['data'].get('baseVolume', 0) for item in buffer_data)
            result['data']['aggregated_volume'] = total_volume
            result['data']['updates_count'] = len(buffer_data)

        elif stream_type == 'trades':
            # For trades, combine all trades
            result = latest.copy()
            all_trades = []
            for item in buffer_data:
                if isinstance(item['data'], list):
                    all_trades.extend(item['data'])
                else:
                    all_trades.append(item['data'])
            result['data'] = all_trades

        else:
            # For orderbook and klines, use latest
            result = latest
            result['updates_count'] = len(buffer_data)

        return result

    async def _distribute_data(self, stream_key: str, data: Dict[str, Any]):
        """Distribute data to all subscribers"""
        tasks = []

        # Send to WebSocket clients
        tasks.append(self._send_to_websocket_clients(stream_key, data))

        # Send to Laravel via RabbitMQ
        tasks.append(self._send_to_laravel(data))

        # Send to local subscribers
        for callback in self.stream_subscriptions[stream_key]:
            tasks.append(self._safe_callback(callback, data))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_to_websocket_clients(self, stream_key: str, data: Dict[str, Any]):
        """Send data to WebSocket clients"""
        try:
            # Get stream components
            parts = stream_key.split(':')
            stream_type = parts[0]
            exchange = parts[1]
            symbol = parts[2]
            timeframe = parts[3] if len(parts) > 3 else None

            # Use appropriate WebSocket handler
            if stream_type == 'ticker':
                await websocket_server._handle_ticker_stream(exchange, symbol, data['data'])
            elif stream_type == 'orderbook':
                await websocket_server._handle_orderbook_stream(exchange, symbol, data['data'])
            elif stream_type == 'klines' and timeframe:
                await websocket_server._handle_klines_stream(exchange, symbol, timeframe, data['data'])
            elif stream_type == 'trades':
                await websocket_server._handle_trades_stream(exchange, symbol, data['data'])

        except Exception as e:
            self.logger.error("Error sending to WebSocket clients", stream=stream_key, error=str(e))

    async def _send_to_laravel(self, data: Dict[str, Any]):
        """Send data to Laravel via RabbitMQ"""
        try:
            if not self.rabbitmq_publisher:
                return

            event_type = f"crypto.{data['type']}.updated"
            routing_key = f"{data['type']}.updated"

            event_data = {
                'event': event_type,
                'exchange': data['exchange'],
                'symbol': data['symbol'],
                'type': data['type'],
                'data': data['data'],
                'timestamp': data['timestamp'],
                'timeframe': data.get('timeframe')
            }

            await self.rabbitmq_publisher.publish(
                exchange='crypto_events',
                routing_key=routing_key,
                message=event_data
            )

        except Exception as e:
            self.logger.error("Error sending to Laravel", error=str(e))

    async def _safe_callback(self, callback: Callable, data: Dict[str, Any]):
        """Safely execute callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error("Callback error", error=str(e))

    async def shutdown(self):
        """Shutdown stream manager"""
        self.running = False

        # Stop all streams
        for stream_key in list(self.active_streams.keys()):
            await self.stop_stream(stream_key)

        # Flush remaining buffers
        for stream_key in list(self.buffers.keys()):
            await self._flush_buffer(stream_key)

        if self.rabbitmq_publisher:
            await self.rabbitmq_publisher.close()

        self.logger.info("Stream manager shutdown complete")

    def get_stats(self) -> Dict[str, Any]:
        """Get stream manager statistics"""
        return {
            'active_streams': len(self.active_streams),
            'buffer_counts': {k: len(v) for k, v in self.buffers.items()},
            'subscription_counts': {k: len(v) for k, v in self.stream_subscriptions.items()},
            'running': self.running
        }


# Global instance
stream_manager = StreamManager()

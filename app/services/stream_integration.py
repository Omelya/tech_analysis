import asyncio
from typing import Dict, List, Optional, Any
import structlog

from .stream_manager import stream_manager
from .data_processor import data_processor
from .websocket_server import websocket_server
from ..config import settings


class StreamIntegration:
    """Integration layer between data processor and WebSocket streaming"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="stream_integration")
        self.active_integrations: Dict[str, str] = {}

    async def initialize(self):
        """Initialize stream integration"""
        await stream_manager.initialize()
        self.logger.info("Stream integration initialized")

    async def start_real_time_streams(self):
        """Start real-time streams for all active trading pairs"""
        try:
            # Get active trading pairs
            active_pairs = await data_processor.get_active_trading_pairs()

            for pair in active_pairs:
                exchange_slug = pair['exchange_slug']
                symbol = pair['symbol']

                # Start essential streams
                await self._start_pair_streams(exchange_slug, symbol)

            self.logger.info("Real-time streams started", pairs_count=len(active_pairs))

        except Exception as e:
            self.logger.error("Failed to start real-time streams", error=str(e))

    async def _start_pair_streams(self, exchange: str, symbol: str):
        """Start streams for a trading pair"""
        try:
            # Start ticker stream (high priority)
            ticker_stream = await stream_manager.start_stream(exchange, symbol, 'ticker')
            self.active_integrations[f"ticker_{exchange}_{symbol}"] = ticker_stream

            # Start orderbook stream (medium priority)
            orderbook_stream = await stream_manager.start_stream(exchange, symbol, 'orderbook')
            self.active_integrations[f"orderbook_{exchange}_{symbol}"] = orderbook_stream

            # Start klines streams for key timeframes
            for timeframe in ['1m', '5m', '1h']:
                klines_stream = await stream_manager.start_stream(exchange, symbol, 'klines', timeframe)
                self.active_integrations[f"klines_{exchange}_{symbol}_{timeframe}"] = klines_stream

        except Exception as e:
            self.logger.error("Failed to start streams for pair",
                              exchange=exchange, symbol=symbol, error=str(e))

    async def add_trading_pair_streams(self, exchange: str, symbol: str):
        """Add streams for new trading pair"""
        await self._start_pair_streams(exchange, symbol)
        self.logger.info("Streams added for new pair", exchange=exchange, symbol=symbol)

    async def remove_trading_pair_streams(self, exchange: str, symbol: str):
        """Remove streams for trading pair"""
        # Find and stop all streams for this pair
        keys_to_remove = []
        for key, stream_key in self.active_integrations.items():
            if f"{exchange}_{symbol}" in key:
                await stream_manager.stop_stream(stream_key)
                keys_to_remove.append(key)

        # Clean up
        for key in keys_to_remove:
            del self.active_integrations[key]

        self.logger.info("Streams removed for pair", exchange=exchange, symbol=symbol)

    async def subscribe_to_pair_updates(self, exchange: str, symbol: str, callback):
        """Subscribe to all updates for a trading pair"""
        streams_to_subscribe = [
            f"ticker:{exchange}:{symbol}",
            f"orderbook:{exchange}:{symbol}",
            f"klines:{exchange}:{symbol}:1m"
        ]

        for stream_key in streams_to_subscribe:
            await stream_manager.subscribe_to_stream(stream_key, callback)

    def get_integration_stats(self) -> Dict[str, Any]:
        """Get integration statistics"""
        return {
            'active_integrations': len(self.active_integrations),
            'integration_types': list(set(key.split('_')[0] for key in self.active_integrations.keys())),
            'stream_manager_stats': stream_manager.get_stats()
        }

    async def shutdown(self):
        """Shutdown stream integration"""
        await stream_manager.shutdown()
        self.active_integrations.clear()
        self.logger.info("Stream integration shutdown complete")


# Global instance
stream_integration = StreamIntegration()

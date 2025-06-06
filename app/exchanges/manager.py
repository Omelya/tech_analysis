import logging
from typing import Dict, List, Optional, Any, Callable, Set
import asyncio
import structlog
from datetime import datetime, timedelta
from collections import defaultdict

from .factory import ExchangeFactory
from .base import ExchangeAdapter, ConnectionStatus
from ..config import settings


class WebSocketSubscriptionManager:
    """Централізований менеджер WebSocket підписок"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="ws_subscription_manager")

        # Callbacks per subscription key
        self.callbacks: Dict[str, List[Callable]] = defaultdict(list)

        # Active subscriptions per exchange
        self.active_subscriptions: Dict[str, Set[str]] = defaultdict(set)

        # Subscription locks to prevent race conditions
        self.subscription_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _get_subscription_key(self, exchange: str, symbol: str, stream_type: str, timeframe: str = None) -> str:
        """Generate unique subscription key"""
        if timeframe:
            return f"{exchange}:{symbol}:{stream_type}:{timeframe}"
        return f"{exchange}:{symbol}:{stream_type}"

    async def add_subscription(self, exchange: str, symbol: str, stream_type: str,
                               callback: Callable, timeframe: str = None) -> str:
        """Add subscription callback"""
        subscription_key = self._get_subscription_key(exchange, symbol, stream_type, timeframe)

        async with self.subscription_locks[exchange]:
            # Add callback
            self.callbacks[subscription_key].append(callback)

            # Track subscription
            self.active_subscriptions[exchange].add(subscription_key)

            self.logger.info("Subscription added",
                             subscription_key=subscription_key,
                             callback_count=len(self.callbacks[subscription_key]))

            return subscription_key

    async def remove_subscription(self, subscription_key: str, callback: Callable = None):
        """Remove subscription callback"""
        if subscription_key not in self.callbacks:
            return

        if callback:
            # Remove specific callback
            if callback in self.callbacks[subscription_key]:
                self.callbacks[subscription_key].remove(callback)
        else:
            # Remove all callbacks
            self.callbacks[subscription_key].clear()

        # Clean up empty subscriptions
        if not self.callbacks[subscription_key]:
            del self.callbacks[subscription_key]

            # Remove from active subscriptions
            exchange = subscription_key.split(':')[0]
            self.active_subscriptions[exchange].discard(subscription_key)

    async def dispatch_data(self, exchange: str, symbol: str, stream_type: str,
                            data: Dict[str, Any], timeframe: str = None):
        """Dispatch data to all subscribers"""
        subscription_key = self._get_subscription_key(exchange, symbol, stream_type, timeframe)

        if subscription_key not in self.callbacks:
            return

        # Dispatch to all callbacks
        tasks = []
        for callback in self.callbacks[subscription_key]:
            tasks.append(self._safe_callback(callback, data))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_callback(self, callback: Callable, data: Dict[str, Any]):
        """Safely execute callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error("Callback execution failed", error=str(e))

    def get_active_subscriptions(self, exchange: str = None) -> Dict[str, Any]:
        """Get active subscriptions info"""
        if exchange:
            return {
                "exchange": exchange,
                "subscriptions": list(self.active_subscriptions[exchange]),
                "subscription_count": len(self.active_subscriptions[exchange])
            }

        return {
            "total_subscriptions": sum(len(subs) for subs in self.active_subscriptions.values()),
            "by_exchange": {
                exchange: {
                    "count": len(subs),
                    "subscriptions": list(subs)
                }
                for exchange, subs in self.active_subscriptions.items()
            }
        }


class ExchangeManager:
    """Оновлений менеджер з централізованим WebSocket управлінням"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="exchange_manager")

        # Single adapter per exchange for public access
        self.public_adapters: Dict[str, ExchangeAdapter] = {}

        # Private adapters per credentials hash
        self.private_adapters: Dict[str, ExchangeAdapter] = {}

        # WebSocket subscription manager
        self.subscription_manager = WebSocketSubscriptionManager()

        # Connection management
        self.health_check_task: Optional[asyncio.Task] = None
        self.rate_limit_monitors: Dict[str, Dict[str, Any]] = {}
        self.connection_locks: Dict[str, asyncio.Lock] = {}

        # WebSocket connection status
        self.ws_connection_status: Dict[str, bool] = {}

    async def initialize(self):
        """Initialize exchange manager"""
        self.logger.info("Initializing exchange manager")

        # Initialize rate limit monitors
        for exchange_id in ExchangeFactory.get_supported_exchanges():
            self.rate_limit_monitors[exchange_id] = {
                'requests': 0,
                'last_reset': datetime.now(),
                'limit': settings.exchange_rate_limits.get(exchange_id, 1000)
            }
            self.connection_locks[exchange_id] = asyncio.Lock()

        # Pre-initialize public adapters for all supported exchanges
        await self._initialize_public_adapters()

        self.health_check_task = asyncio.create_task(self._health_check_loop())
        self.logger.info("Exchange manager initialized")

    async def _initialize_public_adapters(self):
        """Pre-initialize public adapters for all exchanges"""
        for exchange_id in ExchangeFactory.get_supported_exchanges():
            try:
                adapter = await self._create_public_adapter(exchange_id)
                if adapter:
                    self.public_adapters[exchange_id] = adapter
                    self.ws_connection_status[exchange_id] = True
                    self.logger.info("Public adapter initialized", exchange=exchange_id)
                else:
                    self.ws_connection_status[exchange_id] = False
                    self.logger.warning("Failed to initialize public adapter", exchange=exchange_id)
            except Exception as e:
                self.ws_connection_status[exchange_id] = False
                self.logger.error("Error initializing public adapter",
                                  exchange=exchange_id, error=str(e))

    async def _create_public_adapter(self, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Create public adapter for exchange"""
        async with self.connection_locks[exchange_id]:
            adapter = ExchangeFactory.create_adapter(exchange_id)
            if adapter:
                success = await adapter.initialize({}, {})
                if success:
                    # Setup universal data callback for this exchange
                    await self._setup_universal_callback(exchange_id, adapter)
                    return adapter
                else:
                    await adapter.close()
            return None

    async def _setup_universal_callback(self, exchange_id: str, adapter: ExchangeAdapter):
        """Setup universal callback that dispatches to subscription manager"""

        async def universal_callback(data: Dict[str, Any]):
            """Universal callback that routes data to subscription manager"""
            try:
                # Extract data components
                data_type = data.get('type', 'unknown')
                symbol = data.get('symbol', '')
                timeframe = data.get('timeframe')

                # Dispatch to subscription manager
                await self.subscription_manager.dispatch_data(
                    exchange=exchange_id,
                    symbol=symbol,
                    stream_type=data_type,
                    data=data,
                    timeframe=timeframe
                )

            except Exception as e:
                self.logger.error("Universal callback error",
                                  exchange=exchange_id, error=str(e))

        # Store callback reference in adapter for later use
        adapter._universal_callback = universal_callback

    async def get_public_adapter(self, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Get public adapter for exchange (reuse existing)"""
        if exchange_id not in self.public_adapters:
            self.logger.warning("Public adapter not found", exchange=exchange_id)
            return None

        adapter = self.public_adapters[exchange_id]

        # Check if adapter is still connected
        status = await adapter.get_connection_status()

        if status.get('status') != 'connected':
            self.logger.info("Reconnecting public adapter", exchange=exchange_id)
            # Try to reconnect
            new_adapter = await self._create_public_adapter(exchange_id)
            if new_adapter:
                # Close old adapter
                try:
                    await adapter.close()
                except:
                    pass
                self.public_adapters[exchange_id] = new_adapter
                self.ws_connection_status[exchange_id] = True
                return new_adapter
            else:
                self.ws_connection_status[exchange_id] = False
                return None

        return adapter

    async def subscribe_to_stream(self, exchange_id: str, symbol: str, stream_type: str,
                                  callback: Callable, timeframe: str = None) -> Optional[str]:
        """Subscribe to stream using existing adapter with callback multiplexing"""

        # Add subscription to manager first
        subscription_key = await self.subscription_manager.add_subscription(
            exchange_id, symbol, stream_type, callback, timeframe
        )

        # Get or create adapter
        adapter = await self.get_public_adapter(exchange_id)
        if not adapter:
            await self.subscription_manager.remove_subscription(subscription_key, callback)
            return None

        # Check if this is the first subscription for this key
        subscription_info = self.subscription_manager.get_active_subscriptions(exchange_id)
        is_first_subscription = len([sub for sub in subscription_info["subscriptions"]
                                     if sub.startswith(f"{exchange_id}:{symbol}:{stream_type}")]) == 1

        if is_first_subscription:
            # Only create WebSocket subscription if this is the first subscriber
            try:
                success = await self._create_websocket_subscription(
                    adapter, symbol, stream_type, timeframe
                )

                if not success:
                    await self.subscription_manager.remove_subscription(subscription_key, callback)
                    return None

            except Exception as e:
                self.logger.error("Failed to create WebSocket subscription",
                                  exchange=exchange_id, symbol=symbol,
                                  stream_type=stream_type, error=str(e))
                await self.subscription_manager.remove_subscription(subscription_key, callback)
                return None

        self.logger.info("Stream subscription added",
                         subscription_key=subscription_key,
                         is_first=is_first_subscription)

        return subscription_key

    async def _create_websocket_subscription(self, adapter: ExchangeAdapter, symbol: str,
                                             stream_type: str, timeframe: str = None) -> bool:
        """Create actual WebSocket subscription"""
        try:
            # Use the universal callback stored in adapter
            universal_callback = adapter._universal_callback

            if stream_type == 'ticker':
                return await adapter.websocket.subscribe_ticker(symbol, universal_callback)
            elif stream_type == 'orderbook':
                return await adapter.websocket.subscribe_orderbook(symbol, universal_callback)
            elif stream_type == 'klines' and timeframe:
                return await adapter.websocket.subscribe_klines(symbol, timeframe, universal_callback)
            else:
                self.logger.warning("Unknown stream type", stream_type=stream_type)
                return False

        except Exception as e:
            self.logger.error("WebSocket subscription failed", error=str(e))
            return False

    async def unsubscribe_from_stream(self, subscription_key: str, callback: Callable = None) -> bool:
        """Unsubscribe from stream"""
        try:
            # Remove from subscription manager
            await self.subscription_manager.remove_subscription(subscription_key, callback)

            # Check if this was the last subscriber
            parts = subscription_key.split(':')
            if len(parts) < 3:
                return False

            exchange_id = parts[0]
            subscription_info = self.subscription_manager.get_active_subscriptions(exchange_id)

            # If no more subscriptions for this key, unsubscribe from WebSocket
            matching_subs = [sub for sub in subscription_info["subscriptions"]
                             if sub.startswith(':'.join(parts[:3]))]

            if not matching_subs:
                adapter = self.public_adapters.get(exchange_id)
                if adapter:
                    # Unsubscribe from WebSocket (implementation depends on adapter)
                    await adapter.websocket.unsubscribe(subscription_key)

            return True

        except Exception as e:
            self.logger.error("Failed to unsubscribe", error=str(e))
            return False

    async def bulk_subscribe(self, subscriptions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Bulk subscribe to multiple streams efficiently"""
        results = {
            'successful': [],
            'failed': [],
            'total': len(subscriptions)
        }

        # Group by exchange for efficient processing
        by_exchange = defaultdict(list)
        for sub in subscriptions:
            exchange_id = sub['exchange']
            by_exchange[exchange_id].append(sub)

        # Process each exchange
        for exchange_id, exchange_subs in by_exchange.items():
            self.logger.info("Processing bulk subscriptions",
                             exchange=exchange_id, count=len(exchange_subs))

            # Get adapter once per exchange
            adapter = await self.get_public_adapter(exchange_id)
            if not adapter:
                for sub in exchange_subs:
                    results['failed'].append({
                        'subscription': sub,
                        'error': 'Adapter not available'
                    })
                continue

            # Process subscriptions for this exchange
            for sub in exchange_subs:
                try:
                    subscription_key = await self.subscribe_to_stream(
                        exchange_id=sub['exchange'],
                        symbol=sub['symbol'],
                        stream_type=sub['stream_type'],
                        callback=sub['callback'],
                        timeframe=sub.get('timeframe')
                    )

                    if subscription_key:
                        results['successful'].append({
                            'subscription': sub,
                            'subscription_key': subscription_key
                        })
                    else:
                        results['failed'].append({
                            'subscription': sub,
                            'error': 'Subscription failed'
                        })

                except Exception as e:
                    results['failed'].append({
                        'subscription': sub,
                        'error': str(e)
                    })

                # Small delay to prevent overwhelming
                await asyncio.sleep(0.01)

        self.logger.info("Bulk subscription completed",
                         successful=len(results['successful']),
                         failed=len(results['failed']))

        return results

    async def get_subscription_stats(self) -> Dict[str, Any]:
        """Get comprehensive subscription statistics"""
        stats = self.subscription_manager.get_active_subscriptions()

        # Add WebSocket connection status
        stats['websocket_connections'] = {
            exchange: {
                'connected': self.ws_connection_status.get(exchange, False),
                'adapter_available': exchange in self.public_adapters
            }
            for exchange in ExchangeFactory.get_supported_exchanges()
        }

        return stats

    # ... (rest of existing methods remain the same: verify_exchange_credentials,
    #      get_exchange_info, fetch_ticker, etc.)

    async def shutdown(self):
        """Shutdown exchange manager"""
        self.logger.info("Shutting down exchange manager")

        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        # Close all public adapters
        for exchange_id, adapter in self.public_adapters.items():
            try:
                await adapter.close()
                self.logger.info("Closed public adapter", exchange=exchange_id)
            except Exception as e:
                self.logger.error("Error closing public adapter",
                                  exchange=exchange_id, error=str(e))

        # Close all private adapters
        for cache_key, adapter in self.private_adapters.items():
            try:
                await adapter.close()
            except Exception as e:
                self.logger.error("Error closing private adapter",
                                  cache_key=cache_key, error=str(e))

        self.public_adapters.clear()
        self.private_adapters.clear()
        self.logger.info("Exchange manager shutdown complete")

    async def _health_check_loop(self):
        """Health check for all adapters"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                # Check public adapters
                for exchange_id, adapter in list(self.public_adapters.items()):
                    try:
                        status = await adapter.get_connection_status()
                        if status['status'] != 'connected':
                            self.logger.warning("Public adapter disconnected",
                                                exchange=exchange_id)
                            self.ws_connection_status[exchange_id] = False
                        else:
                            self.ws_connection_status[exchange_id] = True
                    except Exception as e:
                        self.logger.error("Health check failed for public adapter",
                                          exchange=exchange_id, error=str(e))
                        self.ws_connection_status[exchange_id] = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Health check loop error", error=str(e))


# Global instance
exchange_manager = ExchangeManager()
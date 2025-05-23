from typing import Dict, List, Optional, Any, Callable
import asyncio
import structlog
from datetime import datetime, timedelta

from .factory import ExchangeFactory
from .base import ExchangeAdapter, ConnectionStatus
from ..config import settings


class ExchangeManager:
    """Manager for handling multiple exchange connections and operations"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="exchange_manager")
        self.adapters: Dict[str, ExchangeAdapter] = {}
        self.health_check_task: Optional[asyncio.Task] = None
        self.rate_limit_monitors: Dict[str, Dict[str, Any]] = {}

    async def initialize(self):
        """Initialize exchange manager"""
        self.logger.info("Initializing exchange manager")

        for exchange_id in ExchangeFactory.get_supported_exchanges():
            self.rate_limit_monitors[exchange_id] = {
                'requests': 0,
                'last_reset': datetime.now(),
                'limit': settings.exchange_rate_limits.get(exchange_id, 1000)
            }

        self.health_check_task = asyncio.create_task(self._health_check_loop())
        self.logger.info("Exchange manager initialized")

    async def shutdown(self):
        """Shutdown exchange manager"""
        self.logger.info("Shutting down exchange manager")

        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass

        await ExchangeFactory.close_all_adapters()
        self.adapters.clear()
        self.logger.info("Exchange manager shutdown complete")

    def get_supported_exchanges(self):
        """Return list of supported exchanges"""
        return list(ExchangeFactory.get_supported_exchanges())

    async def get_adapter(self, exchange_id: str, credentials: Optional[Dict[str, str]] = None,
                          options: Optional[Dict[str, Any]] = None) -> Optional[ExchangeAdapter]:
        """Get or create exchange adapter"""
        try:
            if not self._check_rate_limit(exchange_id):
                self.logger.warning("Rate limit exceeded", exchange=exchange_id)
                return None

            adapter = await ExchangeFactory.get_or_create_adapter(
                exchange_id, credentials, options, use_cache=True
            )

            if adapter:
                cache_key = f"{exchange_id}_{hash(str(credentials)) if credentials else 'public'}"
                self.adapters[cache_key] = adapter
                self._increment_rate_limit(exchange_id)

            return adapter

        except Exception as e:
            self.logger.error("Failed to get adapter", exchange=exchange_id, error=str(e))
            return None

    async def get_public_adapter(self, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Get public (no credentials) adapter for exchange"""
        return await self.get_adapter(exchange_id)

    async def get_private_adapter(self, exchange_id: str, credentials: Dict[str, str]) -> Optional[ExchangeAdapter]:
        """Get private (with credentials) adapter for exchange"""
        return await self.get_adapter(exchange_id, credentials)

    async def verify_exchange_credentials(self, exchange_id: str, credentials: Dict[str, str]) -> bool:
        """Verify exchange API credentials"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials, {'verify_only': True})
            if not adapter:
                return False

            is_valid = await adapter.verify_credentials()
            await adapter.close()

            return is_valid

        except Exception as e:
            self.logger.error("Failed to verify credentials", exchange=exchange_id, error=str(e))
            return False

    async def get_exchange_info(self, exchange_id: str) -> Optional[Dict[str, Any]]:
        """Get exchange information (markets, timeframes, etc.)"""
        try:
            adapter = await self.get_public_adapter(exchange_id)
            if not adapter:
                return None

            markets = await adapter.get_markets()
            timeframes = await adapter.get_timeframes()
            status = await adapter.get_connection_status()

            return {
                'exchange_id': exchange_id,
                'markets': markets,
                'timeframes': timeframes,
                'status': status,
                'supported_symbols': len(markets) if markets else 0,
                'supported_timeframes': len(timeframes) if timeframes else 0
            }

        except Exception as e:
            self.logger.error("Failed to get exchange info", exchange=exchange_id, error=str(e))
            return None

    async def get_all_exchanges_status(self) -> Dict[str, Any]:
        """Get status of all exchanges"""
        statuses = {}

        for exchange_id in ExchangeFactory.get_supported_exchanges():
            try:
                status = await ExchangeFactory.get_adapter_status(exchange_id)
                rate_limit_info = self.rate_limit_monitors.get(exchange_id, {})

                statuses[exchange_id] = {
                    **status,
                    'rate_limit': rate_limit_info
                }

            except Exception as e:
                statuses[exchange_id] = {
                    'exchange_id': exchange_id,
                    'status': 'error',
                    'error': str(e)
                }

        return {
            'total_exchanges': len(statuses),
            'exchanges': statuses,
            'timestamp': datetime.now().isoformat()
        }

    async def fetch_ticker(self, exchange_id: str, symbol: str,
                           credentials: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """Fetch ticker data from exchange"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials)
            if not adapter or not hasattr(adapter.rest, 'fetch_ticker'):
                return None

            ticker = await adapter.rest.fetch_ticker(symbol)

            if ticker:
                return {
                    'exchange': exchange_id,
                    'symbol': symbol,
                    'data': ticker,
                    'timestamp': datetime.now().isoformat()
                }

            return None

        except Exception as e:
            self.logger.error("Failed to fetch ticker", exchange=exchange_id, symbol=symbol, error=str(e))
            return None

    async def fetch_ohlcv(self, exchange_id: str, symbol: str, timeframe: str,
                          since: Optional[int] = None, limit: Optional[int] = None,
                          credentials: Optional[Dict[str, str]] = None) -> Optional[List[List[Any]]]:
        """Fetch OHLCV data from exchange"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials)
            if not adapter or not hasattr(adapter.rest, 'fetch_ohlcv'):
                return None

            ohlcv = await adapter.rest.fetch_ohlcv(symbol, timeframe, since, limit)
            return ohlcv

        except Exception as e:
            self.logger.error("Failed to fetch OHLCV", exchange=exchange_id, symbol=symbol,
                              timeframe=timeframe, error=str(e))
            return None

    async def fetch_order_book(self, exchange_id: str, symbol: str, limit: Optional[int] = None,
                               credentials: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """Fetch order book from exchange"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials)
            if not adapter or not hasattr(adapter.rest, 'fetch_order_book'):
                return None

            orderbook = await adapter.rest.fetch_order_book(symbol, limit)

            if orderbook:
                return {
                    'exchange': exchange_id,
                    'symbol': symbol,
                    'data': orderbook,
                    'timestamp': datetime.now().isoformat()
                }

            return None

        except Exception as e:
            self.logger.error("Failed to fetch order book", exchange=exchange_id, symbol=symbol, error=str(e))
            return None

    async def subscribe_to_stream(self, exchange_id: str, stream_type: str, symbol: str,
                                  callback: Callable, credentials: Optional[Dict[str, str]] = None,
                                  **kwargs) -> Optional[str]:
        """Subscribe to WebSocket stream"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials)
            if not adapter or not hasattr(adapter, 'websocket'):
                return None

            subscription_id = None

            if stream_type == 'ticker':
                success = await adapter.websocket.subscribe_ticker(symbol, callback)
                subscription_id = f"ticker_{symbol}"
            elif stream_type == 'orderbook':
                success = await adapter.websocket.subscribe_orderbook(symbol, callback, **kwargs)
                subscription_id = f"orderbook_{symbol}"
            elif stream_type == 'klines':
                timeframe = kwargs.get('timeframe', '1m')
                success = await adapter.websocket.subscribe_klines(symbol, timeframe, callback)
                subscription_id = f"klines_{symbol}_{timeframe}"
            else:
                self.logger.error("Unsupported stream type", stream_type=stream_type)
                return None

            if success:
                return subscription_id

            return None

        except Exception as e:
            self.logger.error("Failed to subscribe to stream", exchange=exchange_id,
                              stream_type=stream_type, symbol=symbol, error=str(e))
            return None

    async def unsubscribe_from_stream(self, exchange_id: str, subscription_id: str,
                                      credentials: Optional[Dict[str, str]] = None) -> bool:
        """Unsubscribe from WebSocket stream"""
        try:
            adapter = await self.get_adapter(exchange_id, credentials)
            if not adapter or not hasattr(adapter, 'websocket'):
                return False

            return await adapter.websocket.unsubscribe(subscription_id)

        except Exception as e:
            self.logger.error("Failed to unsubscribe from stream", exchange=exchange_id,
                              subscription_id=subscription_id, error=str(e))
            return False

    def _check_rate_limit(self, exchange_id: str) -> bool:
        """Check if exchange rate limit allows new request"""
        if exchange_id not in self.rate_limit_monitors:
            return True

        monitor = self.rate_limit_monitors[exchange_id]
        now = datetime.now()

        if now - monitor['last_reset'] > timedelta(minutes=1):
            monitor['requests'] = 0
            monitor['last_reset'] = now

        return monitor['requests'] < monitor['limit']

    def _increment_rate_limit(self, exchange_id: str):
        """Increment rate limit counter for exchange"""
        if exchange_id in self.rate_limit_monitors:
            self.rate_limit_monitors[exchange_id]['requests'] += 1

    async def _health_check_loop(self):
        """Periodic health check for all adapters"""
        while True:
            try:
                await asyncio.sleep(60)

                for cache_key, adapter in list(self.adapters.items()):
                    try:
                        status = await adapter.get_connection_status()

                        if status['status'] == 'error':
                            self.logger.warning("Adapter in error state", cache_key=cache_key)

                        elif status['status'] == 'disconnected':
                            self.logger.info("Removing disconnected adapter", cache_key=cache_key)
                            await adapter.close()
                            del self.adapters[cache_key]

                    except Exception as e:
                        self.logger.error("Health check failed for adapter",
                                          cache_key=cache_key, error=str(e))

                        try:
                            await adapter.close()
                            del self.adapters[cache_key]
                        except:
                            pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Health check loop error", error=str(e))


exchange_manager = ExchangeManager()

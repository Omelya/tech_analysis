import logging
from typing import Dict, List, Optional, Any, Callable
import asyncio
import structlog
from datetime import datetime, timedelta

from .factory import ExchangeFactory
from .base import ExchangeAdapter, ConnectionStatus
from ..config import settings


class ExchangeManager:
    """Optimized manager for handling exchange connections"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="exchange_manager")

        # Single adapter per exchange for public access
        self.public_adapters: Dict[str, ExchangeAdapter] = {}

        # Private adapters per credentials hash
        self.private_adapters: Dict[str, ExchangeAdapter] = {}

        self.health_check_task: Optional[asyncio.Task] = None
        self.rate_limit_monitors: Dict[str, Dict[str, Any]] = {}

        # Connection locks to prevent concurrent initialization
        self.connection_locks: Dict[str, asyncio.Lock] = {}

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
                    self.logger.info("Public adapter initialized", exchange=exchange_id)
                else:
                    self.logger.warning("Failed to initialize public adapter", exchange=exchange_id)
            except Exception as e:
                self.logger.error("Error initializing public adapter",
                                  exchange=exchange_id, error=str(e))

    async def _create_public_adapter(self, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Create public adapter for exchange"""
        async with self.connection_locks[exchange_id]:
            adapter = ExchangeFactory.create_adapter(exchange_id)
            if adapter:
                success = await adapter.initialize({}, {})
                if success:
                    return adapter
                else:
                    await adapter.close()
            return None

    async def get_public_adapter(self, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Get public adapter for exchange (cached)"""
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
                return new_adapter
            else:
                return None

        return adapter

    async def get_private_adapter(self, exchange_id: str, credentials: Dict[str, str]) -> Optional[ExchangeAdapter]:
        """Get private adapter with credentials"""
        if not self._check_rate_limit(exchange_id):
            self.logger.warning("Rate limit exceeded", exchange=exchange_id)
            return None

        # Create cache key
        cache_key = f"{exchange_id}_{hash(str(sorted(credentials.items())))}"

        # Check if we have cached adapter
        if cache_key in self.private_adapters:
            adapter = self.private_adapters[cache_key]
            status = await adapter.get_connection_status()

            if status['status'] == 'connected':
                self._increment_rate_limit(exchange_id)
                return adapter
            else:
                # Remove disconnected adapter
                try:
                    await adapter.close()
                except:
                    pass
                del self.private_adapters[cache_key]

        # Create new private adapter
        try:
            adapter = ExchangeFactory.create_adapter(exchange_id)
            if not adapter:
                return None

            success = await adapter.initialize(credentials, {})
            if success:
                self.private_adapters[cache_key] = adapter
                self._increment_rate_limit(exchange_id)
                return adapter
            else:
                await adapter.close()
                return None

        except Exception as e:
            self.logger.error("Failed to create private adapter",
                              exchange=exchange_id, error=str(e))
            return None

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
                            # Try to reconnect
                            new_adapter = await self._create_public_adapter(exchange_id)
                            if new_adapter:
                                await adapter.close()
                                self.public_adapters[exchange_id] = new_adapter
                                self.logger.info("Public adapter reconnected",
                                                 exchange=exchange_id)
                    except Exception as e:
                        self.logger.error("Health check failed for public adapter",
                                          exchange=exchange_id, error=str(e))

                # Check private adapters
                for cache_key, adapter in list(self.private_adapters.items()):
                    try:
                        status = await adapter.get_connection_status()
                        if status['status'] != 'connected':
                            self.logger.info("Removing disconnected private adapter",
                                             cache_key=cache_key)
                            await adapter.close()
                            del self.private_adapters[cache_key]
                    except Exception as e:
                        self.logger.error("Health check failed for private adapter",
                                          cache_key=cache_key, error=str(e))

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Health check loop error", error=str(e))

    def _check_rate_limit(self, exchange_id: str) -> bool:
        """Check rate limit for exchange"""
        if exchange_id not in self.rate_limit_monitors:
            return True

        monitor = self.rate_limit_monitors[exchange_id]
        now = datetime.now()

        # Reset counter every minute
        if now - monitor['last_reset'] > timedelta(minutes=1):
            monitor['requests'] = 0
            monitor['last_reset'] = now

        return monitor['requests'] < monitor['limit']

    def _increment_rate_limit(self, exchange_id: str):
        """Increment rate limit counter"""
        if exchange_id in self.rate_limit_monitors:
            self.rate_limit_monitors[exchange_id]['requests'] += 1

    # Остальные методы остаются без изменений...
    async def verify_exchange_credentials(self, exchange_id: str, credentials: Dict[str, str]) -> bool:
        """Verify exchange API credentials"""
        try:
            adapter = await self.get_private_adapter(exchange_id, credentials)
            if not adapter:
                return False
            return await adapter.verify_credentials()
        except Exception as e:
            self.logger.error("Failed to verify credentials", exchange=exchange_id, error=str(e))
            return False

    async def get_exchange_info(self, exchange_id: str) -> Optional[Dict[str, Any]]:
        """Get exchange information"""
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

    async def fetch_ticker(self, exchange_id: str, symbol: str,
                           credentials: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
        """Fetch ticker data"""
        try:
            if credentials:
                adapter = await self.get_private_adapter(exchange_id, credentials)
            else:
                adapter = await self.get_public_adapter(exchange_id)

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

    def get_supported_exchanges(self):
        """Return list of supported exchanges"""
        return list(ExchangeFactory.get_supported_exchanges())

    async def get_all_exchanges_status(self) -> Dict[str, Any]:
        """Get status of all exchanges"""
        statuses = {}

        for exchange_id in self.get_supported_exchanges():
            try:
                public_adapter = self.public_adapters.get(exchange_id)
                if public_adapter:
                    status = await public_adapter.get_connection_status()
                    rate_limit_info = self.rate_limit_monitors.get(exchange_id, {})

                    statuses[exchange_id] = {
                        'public_adapter': status,
                        'rate_limit': rate_limit_info,
                        'private_adapters_count': len([k for k in self.private_adapters.keys()
                                                       if k.startswith(exchange_id)])
                    }
                else:
                    statuses[exchange_id] = {
                        'public_adapter': {'status': 'not_initialized'},
                        'rate_limit': self.rate_limit_monitors.get(exchange_id, {}),
                        'private_adapters_count': 0
                    }
            except Exception as e:
                statuses[exchange_id] = {
                    'status': 'error',
                    'error': str(e)
                }

        return {
            'total_exchanges': len(statuses),
            'exchanges': statuses,
            'timestamp': datetime.now().isoformat()
        }


# Global instance
exchange_manager = ExchangeManager()

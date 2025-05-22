from typing import Dict, Any, Optional, Union
import structlog

from .binance import BinanceAdapter
from .bybit import BybitAdapter
from .whitebit import WhiteBitAdapter
from .base import ExchangeAdapter


class ExchangeFactory:
    """Factory class for creating exchange adapters"""

    _adapters = {
        'binance': BinanceAdapter,
        'bybit': BybitAdapter,
        'whitebit': WhiteBitAdapter
    }

    _instances: Dict[str, ExchangeAdapter] = {}

    @classmethod
    def create_adapter(cls, exchange_id: str, credentials: Optional[Dict[str, str]] = None,
                       options: Optional[Dict[str, Any]] = None) -> Optional[ExchangeAdapter]:
        """Create exchange adapter instance"""
        logger = structlog.get_logger().bind(exchange=exchange_id)

        if exchange_id not in cls._adapters:
            logger.error("Unsupported exchange", supported_exchanges=list(cls._adapters.keys()))
            return None

        try:
            adapter_class = cls._adapters[exchange_id]
            adapter = adapter_class()

            logger.info("Created exchange adapter")
            return adapter

        except Exception as e:
            logger.error("Failed to create exchange adapter", error=str(e))
            return None

    @classmethod
    async def get_or_create_adapter(cls, exchange_id: str, credentials: Optional[Dict[str, str]] = None,
                                    options: Optional[Dict[str, Any]] = None,
                                    use_cache: bool = True) -> Optional[ExchangeAdapter]:
        """Get existing adapter or create new one"""
        logger = structlog.get_logger().bind(exchange=exchange_id)

        cache_key = f"{exchange_id}_{hash(str(credentials)) if credentials else 'public'}"

        if use_cache and cache_key in cls._instances:
            adapter = cls._instances[cache_key]
            status = await adapter.get_connection_status()

            if status['status'] in ['connected', 'connecting']:
                logger.info("Using cached adapter")
                return adapter
            else:
                logger.info("Cached adapter disconnected, creating new one")
                await adapter.close()
                del cls._instances[cache_key]

        adapter = cls.create_adapter(exchange_id, credentials, options)
        if not adapter:
            return None

        if credentials:
            success = await adapter.initialize(credentials, options or {})
            if not success:
                logger.error("Failed to initialize adapter with credentials")
                await adapter.close()
                return None
        else:
            success = await adapter.initialize({}, options or {})
            if not success:
                logger.error("Failed to initialize public adapter")
                await adapter.close()
                return None

        if use_cache:
            cls._instances[cache_key] = adapter

        logger.info("Created and initialized new adapter")
        return adapter

    @classmethod
    def get_supported_exchanges(cls) -> list[str]:
        """Get list of supported exchanges"""
        return list(cls._adapters.keys())

    @classmethod
    async def close_all_adapters(cls):
        """Close all cached adapter instances"""
        logger = structlog.get_logger()

        for cache_key, adapter in cls._instances.items():
            try:
                await adapter.close()
                logger.info("Closed adapter", cache_key=cache_key)
            except Exception as e:
                logger.error("Failed to close adapter", cache_key=cache_key, error=str(e))

        cls._instances.clear()
        logger.info("All adapters closed")

    @classmethod
    async def get_adapter_status(cls, exchange_id: str) -> Dict[str, Any]:
        """Get status of all adapters for specific exchange"""
        status_list = []

        for cache_key, adapter in cls._instances.items():
            if cache_key.startswith(exchange_id):
                try:
                    status = await adapter.get_connection_status()
                    status['cache_key'] = cache_key
                    status_list.append(status)
                except Exception as e:
                    status_list.append({
                        'cache_key': cache_key,
                        'status': 'error',
                        'error': str(e)
                    })

        return {
            'exchange_id': exchange_id,
            'total_adapters': len(status_list),
            'adapters': status_list
        }

    @classmethod
    async def remove_adapter(cls, cache_key: str) -> bool:
        """Remove specific adapter from cache"""
        logger = structlog.get_logger()

        if cache_key in cls._instances:
            try:
                adapter = cls._instances[cache_key]
                await adapter.close()
                del cls._instances[cache_key]
                logger.info("Removed adapter", cache_key=cache_key)
                return True
            except Exception as e:
                logger.error("Failed to remove adapter", cache_key=cache_key, error=str(e))
                return False

        logger.warning("Adapter not found in cache", cache_key=cache_key)
        return False

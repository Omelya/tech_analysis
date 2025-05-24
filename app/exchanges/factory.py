from typing import Dict, Any, Optional, Union
import structlog

from .binance import BinanceAdapter
from .bybit import BybitAdapter
from .whitebit import WhiteBitAdapter
from .base import ExchangeAdapter


class ExchangeFactory:
    """Optimized factory class for creating exchange adapters"""

    _adapters = {
        'binance': BinanceAdapter,
        'bybit': BybitAdapter,
        'whitebit': WhiteBitAdapter
    }

    _instances: Dict[str, ExchangeAdapter] = {}

    @classmethod
    def create_adapter(cls, exchange_id: str) -> Optional[ExchangeAdapter]:
        """Create new exchange adapter instance (no caching here)"""
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
    def get_supported_exchanges(cls) -> list[str]:
        """Get list of supported exchanges"""
        return list(cls._adapters.keys())

    @classmethod
    def validate_exchange_id(cls, exchange_id: str) -> bool:
        """Validate if exchange ID is supported"""
        return exchange_id in cls._adapters

    @classmethod
    def get_adapter_class(cls, exchange_id: str) -> Optional[type]:
        """Get adapter class for exchange"""
        return cls._adapters.get(exchange_id)

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

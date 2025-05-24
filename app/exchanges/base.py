from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import ccxt.async_support as ccxt
import asyncio
import structlog
from enum import Enum


class ConnectionStatus(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"
    RECONNECTING = "reconnecting"


class ExchangeAdapter(ABC):
    """Base abstract class for exchange adapters"""

    def __init__(self, exchange_id: str, config: Dict[str, Any]):
        self.exchange_id = exchange_id
        self.config = config
        self.logger = structlog.get_logger().bind(exchange=exchange_id)
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.exchange: Optional[ccxt.Exchange] = None
        self.last_error: Optional[str] = None
        self.rate_limit_remaining = 1000
        self.supported_timeframes = []
        self.supported_symbols = []

    @abstractmethod
    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize connection to exchange with credentials"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connection to exchange"""
        pass

    @abstractmethod
    async def verify_credentials(self) -> bool:
        """Verify API credentials are valid"""
        pass

    @abstractmethod
    async def get_markets(self) -> Dict[str, Any]:
        """Get available trading pairs"""
        pass

    @abstractmethod
    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes"""
        pass

    async def get_connection_status(self) -> Dict[str, Any]:
        """Get current connection status and statistics"""
        return {
            "exchange_id": self.exchange_id,
            "status": self.connection_status.value,
            "last_error": self.last_error,
            "rate_limit_remaining": self.rate_limit_remaining,
            "supported_timeframes": self.supported_timeframes,
            "supported_symbols_count": len(self.supported_symbols)
        }

    async def handle_rate_limit(self, exception: Exception) -> None:
        """Handle rate limit exceeded exception"""
        self.logger.warning("Rate limit exceeded", error=str(exception))
        await asyncio.sleep(60)

    async def handle_network_error(self, exception: Exception) -> None:
        """Handle network errors and connection issues"""
        self.last_error = str(exception)
        self.connection_status = ConnectionStatus.ERROR
        self.logger.error("Network error occurred", error=str(exception))
        await asyncio.sleep(5)

    async def safe_request(self, func, *args, default=None, **kwargs):
        """Execute API request with error handling"""
        try:
            if self.connection_status != ConnectionStatus.CONNECTED:
                raise Exception(f"Exchange not connected: {self.connection_status.value}")

            result = await func(*args, **kwargs)

            if hasattr(self.exchange, 'rateLimit'):
                self.rate_limit_remaining = getattr(self.exchange, 'rateLimitRemaining', 1000)

            return result

        except ccxt.RateLimitExceeded as e:
            await self.handle_rate_limit(e)
            return default

        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
            await self.handle_network_error(e)
            return default

        except Exception as e:
            self.logger.error("API request failed", error=str(e), function=func.__name__)
            self.last_error = str(e)
            return default


class RestExchangeAdapter(ExchangeAdapter):
    """Base adapter for REST API exchanges"""

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize REST exchange connection"""
        try:
            self.connection_status = ConnectionStatus.CONNECTING

            exchange_class = getattr(ccxt, self.exchange_id)

            config = {
                'apiKey': credentials.get('api_key'),
                'secret': credentials.get('api_secret'),
                'password': credentials.get('passphrase'),
                'enableRateLimit': True,
                'timeout': 30000,
                **(options or {})
            }

            config = {k: v for k, v in config.items() if v is not None}

            self.exchange = exchange_class(config)

            await self.exchange.load_markets()

            if credentials.get('api_key'):
                is_valid = await self.verify_credentials()
                if not is_valid:
                    raise Exception("Invalid API credentials")

            self.supported_timeframes = list(self.exchange.timeframes.keys()) if self.exchange.timeframes else []
            self.supported_symbols = list(self.exchange.markets.keys()) if self.exchange.markets else []

            self.connection_status = ConnectionStatus.CONNECTED
            self.logger.info("Exchange connected successfully")

            return True

        except Exception as e:
            self.connection_status = ConnectionStatus.ERROR
            self.last_error = str(e)
            self.logger.error("Failed to initialize exchange", error=str(e))
            return False

    async def close(self) -> None:
        """Close exchange connection"""
        if self.exchange:
            try:
                await self.exchange.close()
                self.logger.info("Exchange connection closed")
            except Exception as e:
                self.logger.error("Error closing exchange", error=str(e))
            finally:
                self.exchange = None

        self.connection_status = ConnectionStatus.DISCONNECTED

    async def verify_credentials(self) -> bool:
        """Verify API credentials by fetching balance"""
        try:
            await self.exchange.fetch_balance()
            return True
        except Exception as e:
            self.logger.error("Credential verification failed", error=str(e))
            return False

    async def get_markets(self) -> Dict[str, Any]:
        """Get available markets"""
        return await self.safe_request(
            lambda: self.exchange.markets,
            default={}
        )

    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes"""
        return self.supported_timeframes

    async def fetch_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch ticker data for symbol"""
        return await self.safe_request(
            self.exchange.fetch_ticker,
            symbol
        )

    async def fetch_ohlcv(self, symbol: str, timeframe: str, since: Optional[int] = None,
                          limit: Optional[int] = None) -> Optional[List[List[Any]]]:
        """Fetch OHLCV data"""
        return await self.safe_request(
            self.exchange.fetch_ohlcv,
            symbol, timeframe, since, limit
        )

    async def fetch_order_book(self, symbol: str, limit: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Fetch order book"""
        return await self.safe_request(
            self.exchange.fetch_order_book,
            symbol, limit
        )

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None) -> Optional[
        List[Dict[str, Any]]]:
        """Fetch recent trades"""
        return await self.safe_request(
            self.exchange.fetch_trades,
            symbol, since, limit
        )


class WebSocketExchangeAdapter(ExchangeAdapter):
    """Base adapter for WebSocket connections"""

    def __init__(self, exchange_id: str, config: Dict[str, Any]):
        super().__init__(exchange_id, config)
        self.ws_connections = {}
        self.subscriptions = {}

    @abstractmethod
    async def subscribe_ticker(self, symbol: str, callback) -> bool:
        """Subscribe to ticker updates"""
        pass

    @abstractmethod
    async def subscribe_orderbook(self, symbol: str, callback) -> bool:
        """Subscribe to order book updates"""
        pass

    @abstractmethod
    async def subscribe_klines(self, symbol: str, timeframe: str, callback) -> bool:
        """Subscribe to kline/candlestick updates"""
        pass

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from data stream"""
        pass

    async def get_subscriptions(self) -> Dict[str, Any]:
        """Get current subscriptions"""
        return {
            "total_subscriptions": len(self.subscriptions),
            "active_connections": len(self.ws_connections),
            "subscriptions": list(self.subscriptions.keys())
        }

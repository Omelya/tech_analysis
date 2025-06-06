from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List, Optional, Any, Union, Callable
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


class WebSocketExchangeAdapter(ABC):
    """Enhanced base adapter for WebSocket connections with proper lifecycle management"""

    def __init__(self, exchange_id: str, config: Dict[str, Any]):
        super().__init__()
        self.exchange_id = exchange_id
        self.config = config
        self.logger = structlog.get_logger().bind(exchange=exchange_id)
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.last_error: Optional[str] = None

        # Connection management
        self.ws_connection: Optional[Any] = None
        self.connection_lock = asyncio.Lock()

        # Subscription management - single connection, multiple callbacks
        self.active_subscriptions: Dict[str, Dict[str, Any]] = {}
        self.subscription_callbacks: Dict[str, List[Callable]] = defaultdict(list)

        # Message routing
        self.message_handlers: Dict[str, Callable] = {}
        self.universal_callback: Optional[Callable] = None

        # Lifecycle management
        self.connection_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.is_shutting_down = False

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize WebSocket connection"""
        async with self.connection_lock:
            if self.connection_status == ConnectionStatus.CONNECTED:
                return True

            try:
                self.connection_status = ConnectionStatus.CONNECTING
                self.logger.info("Initializing WebSocket connection")

                # Create single WebSocket connection
                success = await self._create_connection()

                if success:
                    self.connection_status = ConnectionStatus.CONNECTED

                    # Start connection maintenance tasks
                    self.connection_task = asyncio.create_task(self._maintain_connection())
                    self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())

                    self.logger.info("WebSocket connection established")
                    return True
                else:
                    self.connection_status = ConnectionStatus.ERROR
                    return False

            except Exception as e:
                self.connection_status = ConnectionStatus.ERROR
                self.last_error = str(e)
                self.logger.error("Failed to initialize WebSocket", error=str(e))
                return False

    async def close(self) -> None:
        """Close WebSocket connection and cleanup"""
        self.is_shutting_down = True

        async with self.connection_lock:
            # Cancel tasks
            for task in [self.connection_task, self.heartbeat_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

            # Close WebSocket connection
            if self.ws_connection:
                try:
                    await self._close_connection()
                except Exception as e:
                    self.logger.error("Error closing WebSocket connection", error=str(e))
                finally:
                    self.ws_connection = None

            # Clear subscriptions
            self.active_subscriptions.clear()
            self.subscription_callbacks.clear()

            self.connection_status = ConnectionStatus.DISCONNECTED
            self.logger.info("WebSocket connection closed")

    async def subscribe_ticker(self, symbol: str, callback: Callable) -> bool:
        """Subscribe to ticker updates with callback multiplexing"""
        return await self._subscribe_with_multiplexing('ticker', symbol, callback)

    async def subscribe_orderbook(self, symbol: str, callback: Callable, depth: int = 20) -> bool:
        """Subscribe to orderbook updates with callback multiplexing"""
        return await self._subscribe_with_multiplexing('orderbook', symbol, callback, depth=depth)

    async def subscribe_klines(self, symbol: str, timeframe: str, callback: Callable) -> bool:
        """Subscribe to klines updates with callback multiplexing"""
        return await self._subscribe_with_multiplexing('klines', symbol, callback, timeframe=timeframe)

    async def _subscribe_with_multiplexing(self, stream_type: str, symbol: str,
                                           callback: Callable, **kwargs) -> bool:
        """Generic subscription with callback multiplexing"""
        if self.connection_status != ConnectionStatus.CONNECTED:
            self.logger.warning("Cannot subscribe - WebSocket not connected",
                                stream_type=stream_type, symbol=symbol)
            return False

        # Create subscription key
        sub_key = self._create_subscription_key(stream_type, symbol, **kwargs)

        async with self.connection_lock:
            # Add callback to multiplexer
            self.subscription_callbacks[sub_key].append(callback)

            # If this is the first callback, create the actual WebSocket subscription
            if len(self.subscription_callbacks[sub_key]) == 1:
                try:
                    success = await self._create_websocket_subscription(stream_type, symbol, **kwargs)

                    if success:
                        self.active_subscriptions[sub_key] = {
                            'stream_type': stream_type,
                            'symbol': symbol,
                            'created_at': datetime.now(),
                            **kwargs
                        }
                        self.logger.info("WebSocket subscription created",
                                         subscription_key=sub_key)
                    else:
                        # Remove callback if subscription failed
                        self.subscription_callbacks[sub_key].remove(callback)
                        if not self.subscription_callbacks[sub_key]:
                            del self.subscription_callbacks[sub_key]
                        return False

                except Exception as e:
                    self.logger.error("Failed to create WebSocket subscription",
                                      subscription_key=sub_key, error=str(e))
                    self.subscription_callbacks[sub_key].remove(callback)
                    if not self.subscription_callbacks[sub_key]:
                        del self.subscription_callbacks[sub_key]
                    return False

            self.logger.info("Callback added to subscription",
                             subscription_key=sub_key,
                             callback_count=len(self.subscription_callbacks[sub_key]))
            return True

    async def unsubscribe(self, subscription_id: str, callback: Callable = None) -> bool:
        """Unsubscribe with proper callback management"""
        async with self.connection_lock:
            if subscription_id not in self.subscription_callbacks:
                return True

            if callback:
                # Remove specific callback
                if callback in self.subscription_callbacks[subscription_id]:
                    self.subscription_callbacks[subscription_id].remove(callback)
            else:
                # Remove all callbacks
                self.subscription_callbacks[subscription_id].clear()

            # If no more callbacks, unsubscribe from WebSocket
            if not self.subscription_callbacks[subscription_id]:
                del self.subscription_callbacks[subscription_id]

                if subscription_id in self.active_subscriptions:
                    await self._remove_websocket_subscription(subscription_id)
                    del self.active_subscriptions[subscription_id]

                self.logger.info("WebSocket subscription removed",
                                 subscription_id=subscription_id)
            else:
                self.logger.info("Callback removed from subscription",
                                 subscription_id=subscription_id,
                                 remaining_callbacks=len(self.subscription_callbacks[subscription_id]))

            return True

    async def _maintain_connection(self):
        """Maintain WebSocket connection"""
        while not self.is_shutting_down:
            try:
                if self.connection_status == ConnectionStatus.CONNECTED:
                    # Check connection health
                    if not await self._is_connection_alive():
                        self.logger.warning("WebSocket connection lost, reconnecting")
                        await self._reconnect()

                await asyncio.sleep(30)  # Check every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Connection maintenance error", error=str(e))
                await asyncio.sleep(10)

    async def _heartbeat_loop(self):
        """Send periodic heartbeat/ping messages"""
        while not self.is_shutting_down:
            try:
                if (self.connection_status == ConnectionStatus.CONNECTED and
                        self.ws_connection):
                    await self._send_heartbeat()

                await asyncio.sleep(20)  # Heartbeat every 20 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Heartbeat error", error=str(e))
                await asyncio.sleep(10)

    async def _reconnect(self):
        """Reconnect WebSocket with subscription recovery"""
        async with self.connection_lock:
            if self.is_shutting_down:
                return

            self.connection_status = ConnectionStatus.RECONNECTING

            # Store current subscriptions for recovery
            subscriptions_to_recover = dict(self.active_subscriptions)

            try:
                # Close existing connection
                if self.ws_connection:
                    await self._close_connection()

                # Create new connection
                success = await self._create_connection()

                if success:
                    self.connection_status = ConnectionStatus.CONNECTED

                    # Recover subscriptions
                    await self._recover_subscriptions(subscriptions_to_recover)

                    self.logger.info("WebSocket reconnected successfully",
                                     recovered_subscriptions=len(subscriptions_to_recover))
                else:
                    self.connection_status = ConnectionStatus.ERROR
                    self.logger.error("Failed to reconnect WebSocket")

            except Exception as e:
                self.connection_status = ConnectionStatus.ERROR
                self.last_error = str(e)
                self.logger.error("Reconnection failed", error=str(e))

    async def _recover_subscriptions(self, subscriptions: Dict[str, Dict[str, Any]]):
        """Recover subscriptions after reconnection"""
        for sub_key, sub_info in subscriptions.items():
            try:
                success = await self._create_websocket_subscription(
                    sub_info['stream_type'],
                    sub_info['symbol'],
                    **{k: v for k, v in sub_info.items()
                       if k not in ['stream_type', 'symbol', 'created_at']}
                )

                if not success:
                    self.logger.error("Failed to recover subscription", subscription_key=sub_key)

            except Exception as e:
                self.logger.error("Error recovering subscription",
                                  subscription_key=sub_key, error=str(e))

    def _create_subscription_key(self, stream_type: str, symbol: str, **kwargs) -> str:
        """Create unique subscription key"""
        key_parts = [stream_type, symbol]

        # Add relevant parameters to key
        if 'timeframe' in kwargs:
            key_parts.append(kwargs['timeframe'])
        if 'depth' in kwargs:
            key_parts.append(str(kwargs['depth']))

        return ':'.join(key_parts)

    async def _dispatch_message_to_callbacks(self, subscription_key: str, data: Dict[str, Any]):
        """Dispatch message to all callbacks for subscription"""
        if subscription_key not in self.subscription_callbacks:
            return

        # Dispatch to all callbacks
        callbacks = list(self.subscription_callbacks[subscription_key])  # Copy to avoid race conditions

        tasks = []
        for callback in callbacks:
            tasks.append(self._safe_callback_execution(callback, data))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_callback_execution(self, callback: Callable, data: Dict[str, Any]):
        """Safely execute callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        except Exception as e:
            self.logger.error("Callback execution failed", error=str(e))

    def get_subscriptions(self) -> Dict[str, Any]:
        """Get current subscriptions info"""
        return {
            "total_subscriptions": len(self.active_subscriptions),
            "subscription_details": {
                sub_key: {
                    **sub_info,
                    "callback_count": len(self.subscription_callbacks.get(sub_key, []))
                }
                for sub_key, sub_info in self.active_subscriptions.items()
            },
            "connection_status": self.connection_status.value
        }

    # Abstract methods that need to be implemented by specific exchanges
    @abstractmethod
    async def _create_connection(self) -> bool:
        """Create WebSocket connection"""
        pass

    @abstractmethod
    async def _close_connection(self):
        """Close WebSocket connection"""
        pass

    @abstractmethod
    async def _create_websocket_subscription(self, stream_type: str, symbol: str, **kwargs) -> bool:
        """Create actual WebSocket subscription"""
        pass

    @abstractmethod
    async def _remove_websocket_subscription(self, subscription_key: str) -> bool:
        """Remove WebSocket subscription"""
        pass

    @abstractmethod
    async def _is_connection_alive(self) -> bool:
        """Check if WebSocket connection is alive"""
        pass

    @abstractmethod
    async def _send_heartbeat(self):
        """Send heartbeat/ping message"""
        pass

    # Default implementations
    async def verify_credentials(self) -> bool:
        """WebSocket doesn't require credential verification for public streams"""
        return True

    async def get_markets(self) -> Dict[str, Any]:
        """Get markets info (not applicable for WebSocket)"""
        return {}

    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes - override in specific adapters"""
        return ['1m', '5m', '15m', '30m', '1h', '4h', '1d']

    async def get_connection_status(self) -> Dict[str, Any]:
        """Get current connection status and statistics"""
        return {
            "exchange_id": self.exchange_id,
            "status": self.connection_status.value,
            "last_error": self.last_error,
            "active_subscriptions": len(self.active_subscriptions),
            "total_callbacks": sum(len(callbacks) for callbacks in self.subscription_callbacks.values()),
            "connection_alive": await self._is_connection_alive() if self.ws_connection else False
        }

from typing import Dict, List, Optional, Any, Callable
import json
import asyncio
import websockets
from datetime import datetime
import ccxt.async_support as ccxt

from .base import RestExchangeAdapter, WebSocketExchangeAdapter, ConnectionStatus


class WhiteBitRestAdapter(RestExchangeAdapter):
    """WhiteBit REST API adapter"""

    def __init__(self):
        super().__init__('whitebit', {
            'rate_limit': 300,
            'weight_limits': {
                'default': 300,
                'orders': 10
            }
        })

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Get WhiteBit exchange information"""
        return await self.safe_request(
            self.exchange.fetch_markets,
            default={}
        )

    async def fetch_server_time(self) -> Optional[int]:
        """Get WhiteBit server time"""
        try:
            result = await self.exchange.fetch_time()
            return result
        except Exception as e:
            self.logger.error("Failed to get server time", error=str(e))
            return None

    async def fetch_market_info(self) -> Optional[Dict[str, Any]]:
        """Fetch market information"""
        return await self.safe_request(
            self.exchange.fetch_markets,
            default={}
        )

    async def fetch_ticker_24h(self, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch 24h ticker data"""
        if symbol:
            return await self.safe_request(
                self.exchange.fetch_ticker,
                symbol
            )
        else:
            return await self.safe_request(
                self.exchange.fetch_tickers,
                default={}
            )

    async def fetch_klines(self, symbol: str, interval: str, start: Optional[int] = None,
                           end: Optional[int] = None, limit: int = 1000) -> Optional[List[List[Any]]]:
        """Fetch kline/candlestick data"""
        params = {}
        if start:
            params['start'] = start
        if end:
            params['end'] = end

        return await self.safe_request(
            self.exchange.fetch_ohlcv,
            symbol, interval, start, limit, params
        )

    async def get_collateral_markets(self) -> Optional[Dict[str, Any]]:
        """Get collateral markets information"""
        try:
            return await self.exchange.publicGetCollateralMarkets()
        except Exception as e:
            self.logger.error("Failed to get collateral markets", error=str(e))
            return {}


class WhiteBitWebSocketAdapter(WebSocketExchangeAdapter):
    """WhiteBit WebSocket API adapter"""

    def __init__(self):
        super().__init__('whitebit', {
            'ws_url': 'wss://api.whitebit.com/ws',
            'ws_public_url': 'wss://api.whitebit.com/ws'
        })
        self.request_id = 1

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize WebSocket connection"""
        try:
            self.connection_status = ConnectionStatus.CONNECTING
            self.logger.info("Initializing WhiteBit WebSocket connection")
            self.connection_status = ConnectionStatus.CONNECTED
            return True
        except Exception as e:
            self.connection_status = ConnectionStatus.ERROR
            self.last_error = str(e)
            self.logger.error("Failed to initialize WebSocket", error=str(e))
            return False

    async def close(self) -> None:
        """Close WebSocket connections"""
        for connection in self.ws_connections.values():
            if connection and not connection.closed:
                await connection.close()

        self.ws_connections.clear()
        self.subscriptions.clear()
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.logger.info("WebSocket connections closed")

    async def verify_credentials(self) -> bool:
        """WebSocket doesn't require credential verification for public streams"""
        return True

    async def get_markets(self) -> Dict[str, Any]:
        """Get markets info (not applicable for WebSocket)"""
        return {}

    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes"""
        return ['1m', '5m', '15m', '30m', '1h', '4h', '12h', '1d', '1w']

    async def _create_connection(self) -> websockets.WebSocketServerProtocol:
        """Create WebSocket connection"""
        connection = await websockets.connect(self.config['ws_public_url'])
        return connection

    async def _send_subscription(self, connection: websockets.WebSocketServerProtocol, method: str, params: List[str]):
        """Send subscription message"""
        message = {
            "id": self.request_id,
            "method": method,
            "params": params
        }
        self.request_id += 1

        await connection.send(json.dumps(message))

    async def _handle_message(self, message: str, callback: Callable):
        try:
            data = json.loads(message)

            if not isinstance(data, dict):
                return

            if 'id' in data and data.get('result') is not None:
                self.logger.info("Subscription response", data=data)
                return

            if 'method' in data and 'params' in data:
                normalized_data = self._normalize_message(data)
                if normalized_data:
                    await callback(normalized_data)
        except Exception as e:
            self.logger.error("Failed to handle WebSocket message", error=str(e))

    def _normalize_message(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit WebSocket message to standard format"""
        method = data.get('method', '')
        params = data.get('params', [])

        if method == 'ticker_update':
            return self._normalize_ticker(params)
        elif method == 'depth_update':
            return self._normalize_orderbook(params)
        elif method == 'candles_update':
            return self._normalize_kline(params)

        return None

    def _normalize_ticker(self, params: List[Any]) -> Dict[str, Any]:
        """Normalize ticker data"""
        if len(params) < 2:
            return None

        symbol = params[0]
        ticker_data = params[1]

        return {
            'type': 'ticker',
            'exchange': 'whitebit',
            'symbol': symbol,
            'data': {
                'symbol': symbol,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'datetime': datetime.now().isoformat(),
                'last': float(ticker_data.get('last_price', 0)),
                'open': float(ticker_data.get('open', 0)),
                'high': float(ticker_data.get('high', 0)),
                'low': float(ticker_data.get('low', 0)),
                'bid': float(ticker_data.get('bid', 0)),
                'ask': float(ticker_data.get('ask', 0)),
                'baseVolume': float(ticker_data.get('base_volume', 0)),
                'quoteVolume': float(ticker_data.get('quote_volume', 0)),
                'change': float(ticker_data.get('change', 0)),
                'percentage': float(ticker_data.get('change_percent', 0))
            }
        }

    def _normalize_orderbook(self, params: List[Any]) -> Dict[str, Any]:
        """Normalize order book data"""
        if len(params) < 3:
            return None

        symbol = params[0]
        is_full = params[1]
        orderbook_data = params[2]

        return {
            'type': 'orderbook',
            'exchange': 'whitebit',
            'symbol': symbol,
            'data': {
                'symbol': symbol,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'datetime': datetime.now().isoformat(),
                'bids': [[float(bid[0]), float(bid[1])] for bid in orderbook_data.get('bids', [])],
                'asks': [[float(ask[0]), float(ask[1])] for ask in orderbook_data.get('asks', [])],
                'is_full_update': is_full
            }
        }

    def _normalize_kline(self, params: List[Any]) -> Dict[str, Any]:
        """Normalize kline data"""
        if len(params) < 3:
            return None

        symbol = params[0]
        interval = params[1]
        kline_data = params[2]

        return {
            'type': 'kline',
            'exchange': 'whitebit',
            'symbol': symbol,
            'timeframe': interval,
            'data': {
                'timestamp': int(kline_data.get('t', 0)) * 1000,
                'datetime': datetime.fromtimestamp(int(kline_data.get('t', 0))).isoformat(),
                'open': float(kline_data.get('o', 0)),
                'high': float(kline_data.get('h', 0)),
                'low': float(kline_data.get('l', 0)),
                'close': float(kline_data.get('c', 0)),
                'volume': float(kline_data.get('v', 0)),
                'is_closed': True
            }
        }

    async def subscribe_ticker(self, symbol: str, callback: Callable) -> bool:
        """Subscribe to ticker updates"""
        try:
            subscription_id = f"ticker_{symbol}"
            connection = await self._create_connection()

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'ticker',
                'symbol': symbol,
                'method': 'ticker_subscribe',
                'callback': callback
            }

            await self._send_subscription(connection, 'ticker_subscribe', [symbol])
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to ticker", symbol=symbol)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to ticker", symbol=symbol, error=str(e))
            return False

    async def subscribe_orderbook(self, symbol: str, callback: Callable, limit: int = 100, interval: str = "0") -> bool:
        """Subscribe to order book updates"""
        try:
            subscription_id = f"orderbook_{symbol}_{limit}"
            connection = await self._create_connection()

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'orderbook',
                'symbol': symbol,
                'method': 'depth_subscribe',
                'callback': callback
            }

            await self._send_subscription(connection, 'depth_subscribe', [symbol, limit, interval])
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to orderbook", symbol=symbol, limit=limit)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to orderbook", symbol=symbol, error=str(e))
            return False

    async def subscribe_klines(self, symbol: str, timeframe: str, callback: Callable) -> bool:
        """Subscribe to kline/candlestick updates"""
        try:
            subscription_id = f"klines_{symbol}_{timeframe}"
            connection = await self._create_connection()

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'klines',
                'symbol': symbol,
                'timeframe': timeframe,
                'method': 'candles_subscribe',
                'callback': callback
            }

            await self._send_subscription(connection, 'candles_subscribe', [symbol, timeframe])
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to klines", symbol=symbol, timeframe=timeframe)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to klines", symbol=symbol, timeframe=timeframe, error=str(e))
            return False

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from data stream"""
        try:
            if subscription_id in self.subscriptions:
                subscription = self.subscriptions[subscription_id]
                connection = self.ws_connections.get(subscription_id)

                if connection and not connection.closed:
                    unsubscribe_method = subscription.get('method', '').replace('_subscribe', '_unsubscribe')
                    symbol = subscription.get('symbol')

                    if unsubscribe_method and symbol:
                        await self._send_subscription(connection, unsubscribe_method, [symbol])

                    await connection.close()

                del self.ws_connections[subscription_id]
                del self.subscriptions[subscription_id]

            self.logger.info("Unsubscribed from stream", subscription_id=subscription_id)
            return True

        except Exception as e:
            self.logger.error("Failed to unsubscribe", subscription_id=subscription_id, error=str(e))
            return False

    async def _listen_to_stream(self, connection: websockets.WebSocketServerProtocol, callback: Callable):
        """Listen to WebSocket stream"""
        try:
            async for message in connection:
                await self._handle_message(message, callback)
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
        except Exception as e:
            self.logger.error("Error in WebSocket stream", error=str(e))


class WhiteBitAdapter:
    """Combined WhiteBit adapter with REST and WebSocket capabilities"""

    def __init__(self):
        self.rest = WhiteBitRestAdapter()
        self.websocket = WhiteBitWebSocketAdapter()
        self.logger = self.rest.logger

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize both REST and WebSocket connections"""
        rest_success = await self.rest.initialize(credentials, options)
        ws_success = await self.websocket.initialize(credentials, options)

        return rest_success and ws_success

    async def close(self) -> None:
        """Close both REST and WebSocket connections"""
        try:
            if self.rest:
                await self.rest.close()
            if self.websocket:
                await self.websocket.close()

            self.logger.info("Adapter closed successfully")
        except Exception as e:
            self.logger.error("Error closing adapter", error=str(e))

    async def get_connection_status(self) -> Dict[str, Any]:
        """Get combined connection status"""
        rest_status = await self.rest.get_connection_status()
        ws_status = await self.websocket.get_connection_status()
        ws_subscriptions = await self.websocket.get_subscriptions()

        return {
            'exchange_id': 'whitebit',
            'rest': rest_status,
            'websocket': ws_status,
            'subscriptions': ws_subscriptions
        }

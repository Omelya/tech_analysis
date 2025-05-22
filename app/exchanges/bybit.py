from typing import Dict, List, Optional, Any, Callable
import json
import asyncio
import websockets
from datetime import datetime
import ccxt.async_support as ccxt

from .base import RestExchangeAdapter, WebSocketExchangeAdapter, ConnectionStatus


class BybitRestAdapter(RestExchangeAdapter):
    """Bybit REST API adapter"""

    def __init__(self):
        super().__init__('bybit', {
            'rate_limit': 600,
            'weight_limits': {
                'default': 600,
                'orders': 20
            }
        })

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Get Bybit exchange information"""
        return await self.safe_request(
            self.exchange.fetch_markets,
            default={}
        )

    async def fetch_instruments_info(self, category: str = 'spot') -> Optional[Dict[str, Any]]:
        """Fetch instruments information"""
        try:
            if hasattr(self.exchange, 'fetch_markets'):
                markets = await self.exchange.fetch_markets()
                return {symbol: market for symbol, market in markets.items()
                        if market.get('type') == category}
            return {}
        except Exception as e:
            self.logger.error("Failed to fetch instruments info", error=str(e))
            return {}

    async def fetch_server_time(self) -> Optional[int]:
        """Get Bybit server time"""
        try:
            result = await self.exchange.fetch_time()
            return result
        except Exception as e:
            self.logger.error("Failed to get server time", error=str(e))
            return None

    async def fetch_klines(self, symbol: str, interval: str, start: Optional[int] = None,
                           end: Optional[int] = None, limit: int = 200) -> Optional[List[List[Any]]]:
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

    async def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Get account information"""
        return await self.safe_request(
            self.exchange.fetch_balance,
            default={}
        )


class BybitWebSocketAdapter(WebSocketExchangeAdapter):
    """Bybit WebSocket API adapter"""

    def __init__(self):
        super().__init__('bybit', {
            'ws_public_spot': 'wss://stream.bybit.com/v5/public/spot',
            'ws_public_linear': 'wss://stream.bybit.com/v5/public/linear',
            'ws_public_inverse': 'wss://stream.bybit.com/v5/public/inverse',
            'ws_private': 'wss://stream.bybit.com/v5/private'
        })
        self.req_id = 1

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize WebSocket connection"""
        try:
            self.connection_status = ConnectionStatus.CONNECTING
            self.logger.info("Initializing Bybit WebSocket connection")
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
        return ['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M']

    def _get_ws_url(self, category: str = 'spot') -> str:
        """Get WebSocket URL based on category"""
        if category == 'linear':
            return self.config['ws_public_linear']
        elif category == 'inverse':
            return self.config['ws_public_inverse']
        else:
            return self.config['ws_public_spot']

    async def _create_connection(self, category: str = 'spot') -> websockets.WebSocketServerProtocol:
        """Create WebSocket connection"""
        url = self._get_ws_url(category)
        connection = await websockets.connect(url)
        return connection

    async def _send_subscription(self, connection: websockets.WebSocketServerProtocol, topic: str, symbol: str):
        """Send subscription message"""
        message = {
            "op": "subscribe",
            "args": [f"{topic}.{symbol}"],
            "req_id": str(self.req_id)
        }
        self.req_id += 1

        await connection.send(json.dumps(message))

    async def _handle_message(self, message: str, callback: Callable):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            if data.get('success') and data.get('op') == 'subscribe':
                self.logger.info("Subscription confirmed", data=data)
                return

            if 'topic' in data and 'data' in data:
                normalized_data = self._normalize_message(data)
                if normalized_data:
                    await callback(normalized_data)

        except Exception as e:
            self.logger.error("Failed to handle WebSocket message", error=str(e), message=message[:100])

    def _normalize_message(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize Bybit WebSocket message to standard format"""
        topic = data.get('topic', '')
        msg_data = data.get('data', {})

        if 'tickers' in topic:
            return self._normalize_ticker(msg_data, topic)
        elif 'orderbook' in topic:
            return self._normalize_orderbook(msg_data, topic)
        elif 'kline' in topic:
            return self._normalize_kline(msg_data, topic)

        return None

    def _normalize_ticker(self, data: Dict[str, Any], topic: str) -> Dict[str, Any]:
        """Normalize ticker data"""
        symbol = topic.split('.')[-1] if '.' in topic else ''

        if isinstance(data, list) and len(data) > 0:
            ticker = data[0]
        else:
            ticker = data

        return {
            'type': 'ticker',
            'exchange': 'bybit',
            'symbol': symbol,
            'data': {
                'symbol': symbol,
                'timestamp': int(ticker.get('ts', 0)),
                'datetime': datetime.fromtimestamp(int(ticker.get('ts', 0)) / 1000).isoformat(),
                'last': float(ticker.get('lastPrice', 0)),
                'open': float(ticker.get('openPrice', 0)),
                'high': float(ticker.get('highPrice24h', 0)),
                'low': float(ticker.get('lowPrice24h', 0)),
                'bid': float(ticker.get('bid1Price', 0)),
                'ask': float(ticker.get('ask1Price', 0)),
                'baseVolume': float(ticker.get('volume24h', 0)),
                'quoteVolume': float(ticker.get('turnover24h', 0)),
                'change': float(ticker.get('price24hPcnt', 0)) * 100,
                'percentage': float(ticker.get('price24hPcnt', 0))
            }
        }

    def _normalize_orderbook(self, data: Dict[str, Any], topic: str) -> Dict[str, Any]:
        """Normalize order book data"""
        symbol = topic.split('.')[-1] if '.' in topic else ''

        if isinstance(data, list) and len(data) > 0:
            orderbook = data[0]
        else:
            orderbook = data

        return {
            'type': 'orderbook',
            'exchange': 'bybit',
            'symbol': symbol,
            'data': {
                'symbol': symbol,
                'timestamp': int(orderbook.get('ts', 0)),
                'datetime': datetime.fromtimestamp(int(orderbook.get('ts', 0)) / 1000).isoformat(),
                'bids': [[float(bid[0]), float(bid[1])] for bid in orderbook.get('b', [])],
                'asks': [[float(ask[0]), float(ask[1])] for ask in orderbook.get('a', [])],
                'nonce': orderbook.get('u')
            }
        }

    def _normalize_kline(self, data: Dict[str, Any], topic: str) -> Dict[str, Any]:
        """Normalize kline data"""
        parts = topic.split('.')
        symbol = parts[-1] if len(parts) > 1 else ''
        timeframe = parts[1] if len(parts) > 2 else ''

        if isinstance(data, list) and len(data) > 0:
            kline = data[0]
        else:
            kline = data

        return {
            'type': 'kline',
            'exchange': 'bybit',
            'symbol': symbol,
            'timeframe': timeframe,
            'data': {
                'timestamp': int(kline.get('start', 0)),
                'datetime': datetime.fromtimestamp(int(kline.get('start', 0)) / 1000).isoformat(),
                'open': float(kline.get('open', 0)),
                'high': float(kline.get('high', 0)),
                'low': float(kline.get('low', 0)),
                'close': float(kline.get('close', 0)),
                'volume': float(kline.get('volume', 0)),
                'is_closed': kline.get('confirm', False)
            }
        }

    async def subscribe_ticker(self, symbol: str, callback: Callable) -> bool:
        """Subscribe to ticker updates"""
        try:
            subscription_id = f"ticker_{symbol}"
            connection = await self._create_connection('spot')

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'ticker',
                'symbol': symbol,
                'topic': f'tickers.{symbol}',
                'callback': callback
            }

            await self._send_subscription(connection, 'tickers', symbol)
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to ticker", symbol=symbol)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to ticker", symbol=symbol, error=str(e))
            return False

    async def subscribe_orderbook(self, symbol: str, callback: Callable, depth: int = 25) -> bool:
        """Subscribe to order book updates"""
        try:
            subscription_id = f"orderbook_{symbol}_{depth}"
            connection = await self._create_connection('spot')

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'orderbook',
                'symbol': symbol,
                'topic': f'orderbook.{depth}.{symbol}',
                'callback': callback
            }

            await self._send_subscription(connection, f'orderbook.{depth}', symbol)
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to orderbook", symbol=symbol, depth=depth)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to orderbook", symbol=symbol, error=str(e))
            return False

    async def subscribe_klines(self, symbol: str, timeframe: str, callback: Callable) -> bool:
        """Subscribe to kline/candlestick updates"""
        try:
            subscription_id = f"klines_{symbol}_{timeframe}"
            connection = await self._create_connection('spot')

            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'klines',
                'symbol': symbol,
                'timeframe': timeframe,
                'topic': f'kline.{timeframe}.{symbol}',
                'callback': callback
            }

            await self._send_subscription(connection, f'kline.{timeframe}', symbol)
            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to klines", symbol=symbol, timeframe=timeframe)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to klines", symbol=symbol, timeframe=timeframe, error=str(e))
            return False

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from data stream"""
        try:
            if subscription_id in self.ws_connections:
                connection = self.ws_connections[subscription_id]
                if connection and not connection.closed:
                    await connection.close()

                del self.ws_connections[subscription_id]

            if subscription_id in self.subscriptions:
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


class BybitAdapter:
    """Combined Bybit adapter with REST and WebSocket capabilities"""

    def __init__(self):
        self.rest = BybitRestAdapter()
        self.websocket = BybitWebSocketAdapter()
        self.logger = self.rest.logger

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize both REST and WebSocket connections"""
        rest_success = await self.rest.initialize(credentials, options)
        ws_success = await self.websocket.initialize(credentials, options)

        return rest_success and ws_success

    async def close(self) -> None:
        """Close both REST and WebSocket connections"""
        await self.rest.close()
        await self.websocket.close()

    async def get_connection_status(self) -> Dict[str, Any]:
        """Get combined connection status"""
        rest_status = await self.rest.get_connection_status()
        ws_status = await self.websocket.get_connection_status()
        ws_subscriptions = await self.websocket.get_subscriptions()

        return {
            'exchange_id': 'bybit',
            'rest': rest_status,
            'websocket': ws_status,
            'subscriptions': ws_subscriptions
        }

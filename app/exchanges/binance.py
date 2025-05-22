from typing import Dict, List, Optional, Any, Callable
import json
import asyncio
import websockets
from datetime import datetime
import ccxt.async_support as ccxt

from .base import RestExchangeAdapter, WebSocketExchangeAdapter, ConnectionStatus


class BinanceRestAdapter(RestExchangeAdapter):
    """Binance REST API adapter"""

    def __init__(self):
        super().__init__('binance', {
            'rate_limit': 1200,
            'weight_limits': {
                'default': 1200,
                'orders': 50
            }
        })

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Get Binance exchange information"""
        return await self.safe_request(
            self.exchange.fetch_markets,
            default={}
        )

    async def fetch_24hr_ticker(self, symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch 24hr ticker statistics"""
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

    async def fetch_klines(self, symbol: str, interval: str, start_time: Optional[int] = None,
                           end_time: Optional[int] = None, limit: int = 500) -> Optional[List[List[Any]]]:
        """Fetch kline/candlestick data"""
        params = {}
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time

        return await self.safe_request(
            self.exchange.fetch_ohlcv,
            symbol, interval, start_time, limit, params
        )

    async def get_server_time(self) -> Optional[int]:
        """Get Binance server time"""
        try:
            result = await self.exchange.fetch_time()
            return result
        except Exception as e:
            self.logger.error("Failed to get server time", error=str(e))
            return None

    async def ping(self) -> bool:
        """Test connectivity to Binance API"""
        try:
            await self.exchange.fetch_time()
            return True
        except Exception:
            return False


class BinanceWebSocketAdapter(WebSocketExchangeAdapter):
    """Binance WebSocket API adapter"""

    def __init__(self):
        super().__init__('binance', {
            'ws_base_url': 'wss://stream.binance.com:9443/ws/',
            'ws_stream_url': 'wss://stream.binance.com:9443/stream'
        })
        self.stream_id = 1

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        """Initialize WebSocket connection"""
        try:
            self.connection_status = ConnectionStatus.CONNECTING
            self.logger.info("Initializing Binance WebSocket connection")
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
        return ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

    async def _create_stream_connection(self, streams: List[str]) -> websockets.WebSocketServerProtocol:
        """Create WebSocket connection for multiple streams"""
        stream_names = '/'.join(streams)
        url = f"{self.config['ws_base_url']}{stream_names}"

        connection = await websockets.connect(url)
        return connection

    async def _handle_message(self, message: str, callback: Callable):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)
            normalized_data = self._normalize_message(data)
            if normalized_data:
                await callback(normalized_data)
        except Exception as e:
            self.logger.error("Failed to handle WebSocket message", error=str(e), message=message[:100])

    def _normalize_message(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize Binance WebSocket message to standard format"""
        if 'stream' in data and 'data' in data:
            stream = data['stream']
            msg_data = data['data']

            if '@ticker' in stream:
                return self._normalize_ticker(msg_data)
            elif '@depth' in stream:
                return self._normalize_orderbook(msg_data)
            elif '@kline' in stream:
                return self._normalize_kline(msg_data)

        return None

    def _normalize_ticker(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize ticker data"""
        return {
            'type': 'ticker',
            'exchange': 'binance',
            'symbol': data.get('s'),
            'data': {
                'symbol': data.get('s'),
                'timestamp': data.get('E'),
                'datetime': datetime.fromtimestamp(data.get('E', 0) / 1000).isoformat(),
                'last': float(data.get('c', 0)),
                'open': float(data.get('o', 0)),
                'high': float(data.get('h', 0)),
                'low': float(data.get('l', 0)),
                'bid': float(data.get('b', 0)),
                'ask': float(data.get('a', 0)),
                'baseVolume': float(data.get('v', 0)),
                'quoteVolume': float(data.get('q', 0)),
                'change': float(data.get('p', 0)),
                'percentage': float(data.get('P', 0))
            }
        }

    def _normalize_orderbook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize order book data"""
        return {
            'type': 'orderbook',
            'exchange': 'binance',
            'symbol': data.get('s'),
            'data': {
                'symbol': data.get('s'),
                'timestamp': data.get('E'),
                'datetime': datetime.fromtimestamp(data.get('E', 0) / 1000).isoformat(),
                'bids': [[float(bid[0]), float(bid[1])] for bid in data.get('b', [])],
                'asks': [[float(ask[0]), float(ask[1])] for ask in data.get('a', [])],
                'nonce': data.get('u')
            }
        }

    def _normalize_kline(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize kline data"""
        kline = data.get('k', {})
        return {
            'type': 'kline',
            'exchange': 'binance',
            'symbol': kline.get('s'),
            'timeframe': kline.get('i'),
            'data': {
                'timestamp': kline.get('t'),
                'datetime': datetime.fromtimestamp(kline.get('t', 0) / 1000).isoformat(),
                'open': float(kline.get('o', 0)),
                'high': float(kline.get('h', 0)),
                'low': float(kline.get('l', 0)),
                'close': float(kline.get('c', 0)),
                'volume': float(kline.get('v', 0)),
                'is_closed': kline.get('x', False)
            }
        }

    async def subscribe_ticker(self, symbol: str, callback: Callable) -> bool:
        """Subscribe to ticker updates"""
        try:
            stream = f"{symbol.lower()}@ticker"
            subscription_id = f"ticker_{symbol}"

            connection = await self._create_stream_connection([stream])
            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'ticker',
                'symbol': symbol,
                'stream': stream,
                'callback': callback
            }

            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to ticker", symbol=symbol)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to ticker", symbol=symbol, error=str(e))
            return False

    async def subscribe_orderbook(self, symbol: str, callback: Callable, levels: int = 20) -> bool:
        """Subscribe to order book updates"""
        try:
            stream = f"{symbol.lower()}@depth{levels}"
            subscription_id = f"orderbook_{symbol}_{levels}"

            connection = await self._create_stream_connection([stream])
            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'orderbook',
                'symbol': symbol,
                'stream': stream,
                'callback': callback
            }

            asyncio.create_task(self._listen_to_stream(connection, callback))

            self.logger.info("Subscribed to orderbook", symbol=symbol, levels=levels)
            return True

        except Exception as e:
            self.logger.error("Failed to subscribe to orderbook", symbol=symbol, error=str(e))
            return False

    async def subscribe_klines(self, symbol: str, timeframe: str, callback: Callable) -> bool:
        """Subscribe to kline/candlestick updates"""
        try:
            stream = f"{symbol.lower()}@kline_{timeframe}"
            subscription_id = f"klines_{symbol}_{timeframe}"

            connection = await self._create_stream_connection([stream])
            self.ws_connections[subscription_id] = connection
            self.subscriptions[subscription_id] = {
                'type': 'klines',
                'symbol': symbol,
                'timeframe': timeframe,
                'stream': stream,
                'callback': callback
            }

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


class BinanceAdapter:
    """Combined Binance adapter with REST and WebSocket capabilities"""

    def __init__(self):
        self.rest = BinanceRestAdapter()
        self.websocket = BinanceWebSocketAdapter()
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
            'exchange_id': 'binance',
            'rest': rest_status,
            'websocket': ws_status,
            'subscriptions': ws_subscriptions
        }

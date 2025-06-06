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
    """Enhanced Binance WebSocket adapter with centralized connection management"""

    def __init__(self):
        super().__init__('binance', {
            'ws_base_url': 'wss://stream.binance.com:9443/ws/',
            'ws_stream_url': 'wss://stream.binance.com:9443/stream'
        })
        self.stream_id = 1

    async def _create_connection(self) -> bool:
        """Create single WebSocket connection for all subscriptions"""
        try:
            # Use combined stream endpoint for multiple subscriptions
            url = self.config['ws_stream_url']

            self.ws_connection = await websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )

            # Start message listening task
            asyncio.create_task(self._listen_to_messages())

            return True

        except Exception as e:
            self.logger.error("Failed to create WebSocket connection", error=str(e))
            return False

    async def _close_connection(self):
        """Close WebSocket connection"""
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()

    async def _create_websocket_subscription(self, stream_type: str, symbol: str, **kwargs) -> bool:
        """Create WebSocket subscription using SUBSCRIBE method"""
        try:
            if not self.ws_connection:
                return False

            # Build stream name based on type
            stream_name = self._build_stream_name(stream_type, symbol, **kwargs)

            # Send subscription message
            subscribe_message = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": self.stream_id
            }

            await self.ws_connection.send(json.dumps(subscribe_message))
            self.stream_id += 1

            self.logger.info("Subscribed to Binance stream",
                             stream_name=stream_name, symbol=symbol)

            return True

        except Exception as e:
            self.logger.error("Failed to create Binance subscription",
                              stream_type=stream_type, symbol=symbol, error=str(e))
            return False

    async def _remove_websocket_subscription(self, subscription_key: str) -> bool:
        """Remove WebSocket subscription using UNSUBSCRIBE method"""
        try:
            if not self.ws_connection:
                return False

            # Parse subscription key to get stream info
            parts = subscription_key.split(':')
            if len(parts) < 2:
                return False

            stream_type = parts[0]
            symbol = parts[1]
            kwargs = {}

            if len(parts) > 2:
                if stream_type == 'klines':
                    kwargs['timeframe'] = parts[2]
                elif stream_type == 'orderbook':
                    kwargs['depth'] = int(parts[2]) if parts[2].isdigit() else 20

            stream_name = self._build_stream_name(stream_type, symbol, **kwargs)

            # Send unsubscription message
            unsubscribe_message = {
                "method": "UNSUBSCRIBE",
                "params": [stream_name],
                "id": self.stream_id
            }

            await self.ws_connection.send(json.dumps(unsubscribe_message))
            self.stream_id += 1

            self.logger.info("Unsubscribed from Binance stream",
                             stream_name=stream_name)

            return True

        except Exception as e:
            self.logger.error("Failed to remove Binance subscription",
                              subscription_key=subscription_key, error=str(e))
            return False

    def _build_stream_name(self, stream_type: str, symbol: str, **kwargs) -> str:
        """Build Binance stream name"""
        symbol_lower = symbol.lower()

        if stream_type == 'ticker':
            return f"{symbol_lower}@ticker"
        elif stream_type == 'orderbook':
            depth = kwargs.get('depth', 20)
            return f"{symbol_lower}@depth{depth}"
        elif stream_type == 'klines':
            timeframe = kwargs.get('timeframe', '1m')
            return f"{symbol_lower}@kline_{timeframe}"
        elif stream_type == 'trades':
            return f"{symbol_lower}@trade"
        else:
            return f"{symbol_lower}@ticker"  # Default

    async def _listen_to_messages(self):
        """Listen to WebSocket messages and dispatch to callbacks"""
        try:
            async for message in self.ws_connection:
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            if not self.is_shutting_down:
                self.logger.warning("Binance WebSocket connection closed")
                self.connection_status = ConnectionStatus.ERROR
        except Exception as e:
            if not self.is_shutting_down:
                self.logger.error("Error in Binance WebSocket listener", error=str(e))
                self.connection_status = ConnectionStatus.ERROR

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            # Skip subscription confirmation messages
            if 'id' in data and 'result' in data:
                return

            # Handle stream data
            if 'stream' in data and 'data' in data:
                stream_name = data['stream']
                msg_data = data['data']

                # Parse stream name to get subscription key
                subscription_key = self._parse_stream_name(stream_name)

                if subscription_key:
                    # Normalize message data
                    normalized_data = self._normalize_message(stream_name, msg_data)

                    if normalized_data:
                        # Dispatch to callbacks
                        await self._dispatch_message_to_callbacks(subscription_key, normalized_data)

        except Exception as e:
            self.logger.error("Failed to handle Binance message",
                              message=message[:100], error=str(e))

    def _parse_stream_name(self, stream_name: str) -> Optional[str]:
        """Parse stream name to subscription key"""
        try:
            parts = stream_name.split('@')
            if len(parts) != 2:
                return None

            symbol = parts[0].upper()
            stream_info = parts[1]

            if stream_info == 'ticker':
                return f"ticker:{symbol}"
            elif stream_info.startswith('depth'):
                depth = stream_info.replace('depth', '') or '20'
                return f"orderbook:{symbol}:{depth}"
            elif stream_info.startswith('kline_'):
                timeframe = stream_info.replace('kline_', '')
                return f"klines:{symbol}:{timeframe}"
            elif stream_info == 'trade':
                return f"trades:{symbol}"

            return None

        except Exception:
            return None

    def _normalize_message(self, stream_name: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize Binance WebSocket message to standard format"""
        try:
            stream_info = stream_name.split('@')[1]

            if stream_info == 'ticker':
                return self._normalize_ticker(data)
            elif stream_info.startswith('depth'):
                return self._normalize_orderbook(data)
            elif stream_info.startswith('kline_'):
                return self._normalize_kline(data)
            elif stream_info == 'trade':
                return self._normalize_trade(data)

            return None

        except Exception as e:
            self.logger.error("Failed to normalize Binance message", error=str(e))
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
            'type': 'klines',
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

    def _normalize_trade(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize trade data"""
        return {
            'type': 'trades',
            'exchange': 'binance',
            'symbol': data.get('s'),
            'data': {
                'id': data.get('t'),
                'timestamp': data.get('T'),
                'datetime': datetime.fromtimestamp(data.get('T', 0) / 1000).isoformat(),
                'symbol': data.get('s'),
                'price': float(data.get('p', 0)),
                'amount': float(data.get('q', 0)),
                'side': 'sell' if data.get('m') else 'buy'
            }
        }

    async def _is_connection_alive(self) -> bool:
        """Check if WebSocket connection is alive"""
        return self.ws_connection and not self.ws_connection.closed

    async def _send_heartbeat(self):
        """Send ping to maintain connection"""
        try:
            if self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.ping()
        except Exception as e:
            self.logger.error("Failed to send heartbeat", error=str(e))

    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes for Binance"""
        return ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']


class BinanceAdapter:
    """Combined Binance adapter with REST and enhanced WebSocket"""

    def __init__(self):
        self.rest = BinanceRestAdapter()
        self.websocket = BinanceWebSocketAdapter()
        self.logger = self.rest.logger

        # Store universal callback reference
        self._universal_callback = None

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

            self.logger.info("Binance adapter closed successfully")
        except Exception as e:
            self.logger.error("Error closing Binance adapter", error=str(e))

    async def get_connection_status(self) -> Dict[str, Any]:
        """Get combined connection status"""
        rest_status = await self.rest.get_connection_status()
        ws_status = await self.websocket.get_connection_status()
        ws_subscriptions = self.websocket.get_subscriptions()

        return {
            'exchange_id': 'binance',
            'rest': rest_status,
            'websocket': ws_status,
            'subscriptions': ws_subscriptions
        }

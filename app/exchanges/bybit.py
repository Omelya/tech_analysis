from abc import ABC
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


class BybitWebSocketAdapter(WebSocketExchangeAdapter, ABC):
    """Enhanced Bybit WebSocket adapter with centralized connection management"""

    def __init__(self):
        super().__init__('bybit', {
            'ws_public_spot': 'wss://stream.bybit.com/v5/public/spot',
            'ws_public_linear': 'wss://stream.bybit.com/v5/public/linear',
            'ws_public_inverse': 'wss://stream.bybit.com/v5/public/inverse'
        })
        self.req_id = 1

    async def _create_connection(self) -> bool:
        """Create single WebSocket connection for all subscriptions"""
        try:
            # Use spot WebSocket by default
            url = self.config['ws_public_spot']

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
            self.logger.error("Failed to create Bybit WebSocket connection", error=str(e))
            return False

    async def _create_websocket_subscription(self, stream_type: str, symbol: str, **kwargs) -> bool:
        """Create WebSocket subscription using Bybit's subscribe method"""
        try:
            if not self.ws_connection:
                return False

            # Build topic based on stream type
            topic = self._build_topic_name(stream_type, symbol, **kwargs)

            # Send subscription message
            subscribe_message = {
                "op": "subscribe",
                "args": [topic],
                "req_id": str(self.req_id)
            }

            await self.ws_connection.send(json.dumps(subscribe_message))
            self.req_id += 1

            self.logger.info("Subscribed to Bybit stream",
                             topic=topic, symbol=symbol)

            return True

        except Exception as e:
            self.logger.error("Failed to create Bybit subscription",
                              stream_type=stream_type, symbol=symbol, error=str(e))
            return False

    async def _remove_websocket_subscription(self, subscription_key: str) -> bool:
        """Remove WebSocket subscription using Bybit's unsubscribe method"""
        try:
            if not self.ws_connection:
                return False

            # Parse subscription key to get topic
            topic = self._subscription_key_to_topic(subscription_key)

            # Send unsubscription message
            unsubscribe_message = {
                "op": "unsubscribe",
                "args": [topic],
                "req_id": str(self.req_id)
            }

            await self.ws_connection.send(json.dumps(unsubscribe_message))
            self.req_id += 1

            self.logger.info("Unsubscribed from Bybit stream", topic=topic)
            return True

        except Exception as e:
            self.logger.error("Failed to remove Bybit subscription",
                              subscription_key=subscription_key, error=str(e))
            return False

    def _build_topic_name(self, stream_type: str, symbol: str, **kwargs) -> str:
        """Build Bybit topic name"""
        if stream_type == 'ticker':
            return f"tickers.{symbol}"
        elif stream_type == 'orderbook':
            depth = kwargs.get('depth', 25)
            return f"orderbook.{depth}.{symbol}"
        elif stream_type == 'klines':
            timeframe = kwargs.get('timeframe', '1')
            return f"kline.{timeframe}.{symbol}"
        elif stream_type == 'trades':
            return f"publicTrade.{symbol}"
        else:
            return f"tickers.{symbol}"  # Default

    def _subscription_key_to_topic(self, subscription_key: str) -> str:
        """Convert subscription key back to Bybit topic"""
        parts = subscription_key.split(':')
        if len(parts) < 2:
            return ""

        stream_type = parts[0]
        symbol = parts[1]

        kwargs = {}
        if len(parts) > 2:
            if stream_type == 'klines':
                kwargs['timeframe'] = parts[2]
            elif stream_type == 'orderbook':
                kwargs['depth'] = int(parts[2]) if parts[2].isdigit() else 25

        return self._build_topic_name(stream_type, symbol, **kwargs)

    async def _listen_to_messages(self):
        """Listen to WebSocket messages and dispatch to callbacks"""
        try:
            async for message in self.ws_connection:
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            if not self.is_shutting_down:
                self.logger.warning("Bybit WebSocket connection closed")
                self.connection_status = ConnectionStatus.ERROR
        except Exception as e:
            if not self.is_shutting_down:
                self.logger.error("Error in Bybit WebSocket listener", error=str(e))
                self.connection_status = ConnectionStatus.ERROR

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message"""
        try:
            data = json.loads(message)

            # Skip subscription confirmation messages
            if 'success' in data and data.get('op') in ['subscribe', 'unsubscribe']:
                return

            # Handle stream data
            if 'topic' in data and 'data' in data:
                topic = data['topic']
                msg_data = data['data']

                # Parse topic to get subscription key
                subscription_key = self._parse_topic(topic)

                if subscription_key:
                    # Normalize message data
                    normalized_data = self._normalize_message(topic, msg_data)

                    if normalized_data:
                        # Dispatch to callbacks
                        await self._dispatch_message_to_callbacks(subscription_key, normalized_data)

        except Exception as e:
            self.logger.error("Failed to handle Bybit message",
                              message=message[:100], error=str(e))

    def _parse_topic(self, topic: str) -> Optional[str]:
        """Parse Bybit topic to subscription key"""
        try:
            parts = topic.split('.')
            if len(parts) < 2:
                return None

            if topic.startswith('tickers'):
                symbol = parts[1]
                return f"ticker:{symbol}"
            elif topic.startswith('orderbook'):
                depth = parts[1]
                symbol = parts[2]
                return f"orderbook:{symbol}:{depth}"
            elif topic.startswith('kline'):
                timeframe = parts[1]
                symbol = parts[2]
                return f"klines:{symbol}:{timeframe}"
            elif topic.startswith('publicTrade'):
                symbol = parts[1]
                return f"trades:{symbol}"

            return None

        except Exception:
            return None

    async def _is_connection_alive(self) -> bool:
        """Check if WebSocket connection is alive"""
        return self.ws_connection and not self.ws_connection.closed

    async def _send_heartbeat(self):
        """Send ping to maintain connection"""
        try:
            if self.ws_connection and not self.ws_connection.closed:
                ping_message = {"op": "ping", "req_id": str(self.req_id)}
                await self.ws_connection.send(json.dumps(ping_message))
                self.req_id += 1
        except Exception as e:
            self.logger.error("Failed to send Bybit heartbeat", error=str(e))

    # Normalization methods (similar to Binance but adapted for Bybit format)
    def _normalize_message(self, topic: str, data: Any) -> Optional[Dict[str, Any]]:
        """Normalize Bybit message format"""
        try:
            if topic.startswith('tickers'):
                return self._normalize_ticker(data, topic)
            elif topic.startswith('orderbook'):
                return self._normalize_orderbook(data, topic)
            elif topic.startswith('kline'):
                return self._normalize_kline(data, topic)
            elif topic.startswith('publicTrade'):
                return self._normalize_trade(data, topic)

            return None

        except Exception as e:
            self.logger.error("Failed to normalize Bybit message", error=str(e))
            return None

    def _normalize_ticker(self, data: Any, topic: str) -> Dict[str, Any]:
        """Normalize Bybit ticker data"""
        symbol = topic.split('.')[1]

        # Handle both single ticker and array format
        ticker = data[0] if isinstance(data, list) and len(data) > 0 else data

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

    def _normalize_orderbook(self, data: Any, topic: str) -> Dict[str, Any]:
        """Normalize Bybit order book data"""
        symbol = topic.split('.')[2] if len(topic.split('.')) > 2 else topic.split('.')[1]

        # Handle both single orderbook and array format
        orderbook = data[0] if isinstance(data, list) and len(data) > 0 else data

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
                'nonce': orderbook.get('u', 0)
            }
        }

    def _normalize_kline(self, data: Any, topic: str) -> Dict[str, Any]:
        """Normalize Bybit kline data"""
        parts = topic.split('.')
        timeframe = parts[1] if len(parts) > 1 else '1'
        symbol = parts[2] if len(parts) > 2 else ''

        # Handle both single kline and array format
        kline = data[0] if isinstance(data, list) and len(data) > 0 else data

        return {
            'type': 'klines',
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

    def _normalize_trade(self, data: Any, topic: str) -> Dict[str, Any]:
        """Normalize Bybit trade data"""
        symbol = topic.split('.')[1] if len(topic.split('.')) > 1 else ''

        # Handle both single trade and array format
        trades = data if isinstance(data, list) else [data]

        normalized_trades = []
        for trade in trades:
            normalized_trades.append({
                'id': trade.get('i', ''),
                'timestamp': int(trade.get('T', 0)),
                'datetime': datetime.fromtimestamp(int(trade.get('T', 0)) / 1000).isoformat(),
                'symbol': symbol,
                'price': float(trade.get('p', 0)),
                'amount': float(trade.get('v', 0)),
                'side': trade.get('S', '').lower()  # 'Buy' or 'Sell' -> 'buy' or 'sell'
            })

        return {
            'type': 'trades',
            'exchange': 'bybit',
            'symbol': symbol,
            'data': normalized_trades if len(normalized_trades) > 1 else normalized_trades[0]
        }

class BybitAdapter:
    """Enhanced Bybit adapter with centralized WebSocket"""

    def __init__(self):
        self.rest = BybitRestAdapter()
        self.websocket = BybitWebSocketAdapter()
        self.logger = self.rest.logger
        self._universal_callback = None

    async def initialize(self, credentials: Dict[str, str], options: Dict[str, Any] = None) -> bool:
        rest_success = await self.rest.initialize(credentials, options)
        ws_success = await self.websocket.initialize(credentials, options)
        return rest_success and ws_success

    async def close(self) -> None:
        try:
            if self.rest:
                await self.rest.close()
            if self.websocket:
                await self.websocket.close()
            self.logger.info("Bybit adapter closed successfully")
        except Exception as e:
            self.logger.error("Error closing Bybit adapter", error=str(e))

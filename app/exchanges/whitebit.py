from abc import ABC
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


class WhiteBitWebSocketAdapter(WebSocketExchangeAdapter, ABC):
    """Enhanced WhiteBit WebSocket adapter with centralized connection management"""

    def __init__(self):
        super().__init__('whitebit', {
            'ws_url': 'wss://api.whitebit.com/ws',
            'ws_public_url': 'wss://api.whitebit.com/ws'
        })
        self.request_id = 1

    async def _create_connection(self) -> bool:
        """Create single WebSocket connection for all subscriptions"""
        try:
            url = self.config['ws_public_url']

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
            self.logger.error("Failed to create WhiteBit WebSocket connection", error=str(e))
            return False

    async def _create_websocket_subscription(self, stream_type: str, symbol: str, **kwargs) -> bool:
        """Create WebSocket subscription using WhiteBit's subscribe method"""
        try:
            if not self.ws_connection:
                return False

            # Build subscription parameters
            method, params = self._build_subscription_params(stream_type, symbol, **kwargs)

            # Send subscription message
            subscribe_message = {
                "id": self.request_id,
                "method": method,
                "params": params
            }

            await self.ws_connection.send(json.dumps(subscribe_message))
            self.request_id += 1

            self.logger.info("Subscribed to WhiteBit stream",
                             method=method, symbol=symbol)

            return True

        except Exception as e:
            self.logger.error("Failed to create WhiteBit subscription",
                              stream_type=stream_type, symbol=symbol, error=str(e))
            return False

    def _build_subscription_params(self, stream_type: str, symbol: str, **kwargs):
        """Build WhiteBit subscription method and parameters"""
        if stream_type == 'ticker':
            return "ticker_subscribe", [symbol]
        elif stream_type == 'orderbook':
            limit = kwargs.get('depth', 100)
            interval = kwargs.get('interval', "0")
            return "depth_subscribe", [symbol, limit, interval]
        elif stream_type == 'klines':
            timeframe = kwargs.get('timeframe', '1m')
            return "candles_subscribe", [symbol, timeframe]
        elif stream_type == 'trades':
            return "trades_subscribe", [symbol]
        else:
            return "ticker_subscribe", [symbol]  # Default

    async def _listen_to_messages(self):
        """Listen to WebSocket messages and dispatch to callbacks"""
        try:
            async for message in self.ws_connection:
                await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed:
            if not self.is_shutting_down:
                self.logger.warning("WhiteBit WebSocket connection closed")
                self.connection_status = ConnectionStatus.ERROR
        except Exception as e:
            if not self.is_shutting_down:
                self.logger.error("Error in WhiteBit WebSocket listener", error=str(e))
                self.connection_status = ConnectionStatus.ERROR

    async def _handle_message(self, message: str):
        """Handle incoming WhiteBit WebSocket message"""
        try:
            data = json.loads(message)

            # Skip subscription confirmation messages
            if 'id' in data and 'result' in data:
                return

            # Handle stream data
            if 'method' in data and 'params' in data:
                method = data['method']
                params = data['params']

                # Parse method to get subscription key
                subscription_key = self._parse_method(method, params)

                if subscription_key:
                    # Normalize message data
                    normalized_data = self._normalize_message(method, params)

                    if normalized_data:
                        # Dispatch to callbacks
                        await self._dispatch_message_to_callbacks(subscription_key, normalized_data)

        except Exception as e:
            self.logger.error("Failed to handle WhiteBit message",
                              message=message[:100], error=str(e))

    def _parse_method(self, method: str, params: List[Any]) -> Optional[str]:
        """Parse WhiteBit method to subscription key"""
        try:
            if not params:
                return None

            symbol = params[0] if len(params) > 0 else ""

            if method == 'ticker_update':
                return f"ticker:{symbol}"
            elif method == 'depth_update':
                return f"orderbook:{symbol}"
            elif method == 'candles_update':
                return f"klines:{symbol}"
            elif method == 'trades_update':
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
                ping_message = {"id": self.request_id, "method": "ping", "params": []}
                await self.ws_connection.send(json.dumps(ping_message))
                self.request_id += 1
        except Exception as e:
            self.logger.error("Failed to send WhiteBit heartbeat", error=str(e))

    # Add normalization methods similar to Binance/Bybit for WhiteBit
    def _normalize_message(self, method: str, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit WebSocket message to standard format"""
        try:
            if method == 'ticker_update':
                return self._normalize_ticker(params)
            elif method == 'depth_update':
                return self._normalize_orderbook(params)
            elif method == 'candles_update':
                return self._normalize_kline(params)
            elif method == 'trades_update':
                return self._normalize_trade(params)

            return None

        except Exception as e:
            self.logger.error("Failed to normalize WhiteBit message", error=str(e))
            return None

    def _normalize_ticker(self, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit ticker data"""
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

    def _normalize_orderbook(self, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit order book data"""
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
                'is_full_update': is_full,
                'nonce': orderbook_data.get('id', 0)
            }
        }

    def _normalize_kline(self, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit kline data"""
        if len(params) < 3:
            return None

        symbol = params[0]
        interval = params[1]
        kline_data = params[2]

        # Handle both single kline and array format
        if isinstance(kline_data, list) and len(kline_data) > 0:
            kline = kline_data[0] if isinstance(kline_data[0], dict) else {
                't': kline_data[0],  # timestamp
                'o': kline_data[1],  # open
                'h': kline_data[2],  # high
                'l': kline_data[3],  # low
                'c': kline_data[4],  # close
                'v': kline_data[5]  # volume
            }
        else:
            kline = kline_data

        return {
            'type': 'klines',
            'exchange': 'whitebit',
            'symbol': symbol,
            'timeframe': interval,
            'data': {
                'timestamp': int(kline.get('t', 0)) * 1000,  # Convert to milliseconds
                'datetime': datetime.fromtimestamp(int(kline.get('t', 0))).isoformat(),
                'open': float(kline.get('o', 0)),
                'high': float(kline.get('h', 0)),
                'low': float(kline.get('l', 0)),
                'close': float(kline.get('c', 0)),
                'volume': float(kline.get('v', 0)),
                'is_closed': True  # WhiteBit usually sends completed candles
            }
        }

    def _normalize_trade(self, params: List[Any]) -> Optional[Dict[str, Any]]:
        """Normalize WhiteBit trade data"""
        if len(params) < 2:
            return None

        symbol = params[0]
        trades_data = params[1]

        # Handle both single trade and array format
        trades = trades_data if isinstance(trades_data, list) else [trades_data]

        normalized_trades = []
        for trade in trades:
            if isinstance(trade, dict):
                normalized_trades.append({
                    'id': trade.get('id', ''),
                    'timestamp': int(trade.get('time', 0)) * 1000,
                    'datetime': datetime.fromtimestamp(int(trade.get('time', 0))).isoformat(),
                    'symbol': symbol,
                    'price': float(trade.get('price', 0)),
                    'amount': float(trade.get('amount', 0)),
                    'side': trade.get('type', 'unknown').lower()  # 'buy' or 'sell'
                })
            elif isinstance(trade, list) and len(trade) >= 5:
                # Array format: [id, timestamp, price, amount, side]
                normalized_trades.append({
                    'id': str(trade[0]),
                    'timestamp': int(trade[1]) * 1000,
                    'datetime': datetime.fromtimestamp(int(trade[1])).isoformat(),
                    'symbol': symbol,
                    'price': float(trade[2]),
                    'amount': float(trade[3]),
                    'side': 'buy' if trade[4] else 'sell'  # True = buy, False = sell
                })

        return {
            'type': 'trades',
            'exchange': 'whitebit',
            'symbol': symbol,
            'data': normalized_trades if len(normalized_trades) > 1 else normalized_trades[
                0] if normalized_trades else {}
        }

    async def get_timeframes(self) -> List[str]:
        """Get supported timeframes for WhiteBit"""
        return ['1m', '5m', '15m', '30m', '1h', '4h', '12h', '1d', '1w']


class WhiteBitAdapter:
    """Enhanced WhiteBit adapter with centralized WebSocket"""

    def __init__(self):
        self.rest = WhiteBitRestAdapter()
        self.websocket = WhiteBitWebSocketAdapter()
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
            self.logger.info("WhiteBit adapter closed successfully")
        except Exception as e:
            self.logger.error("Error closing WhiteBit adapter", error=str(e))

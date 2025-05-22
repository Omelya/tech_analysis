import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from app.exchanges.binance import BinanceAdapter
from app.exchanges.bybit import BybitAdapter
from app.exchanges.whitebit import WhiteBitAdapter
from app.exchanges.factory import ExchangeFactory
from app.exchanges.manager import ExchangeManager


class TestExchangeAdapters:
    """Test exchange adapters functionality"""

    @pytest.mark.asyncio
    async def test_binance_adapter_initialization(self):
        """Test Binance adapter initialization"""
        adapter = BinanceAdapter()

        assert adapter.rest.exchange_id == 'binance'
        assert adapter.websocket.exchange_id == 'binance'

        success = await adapter.initialize({})
        assert success is True

        status = await adapter.get_connection_status()
        assert status['exchange_id'] == 'binance'
        assert 'rest' in status
        assert 'websocket' in status

        await adapter.close()

    @pytest.mark.asyncio
    async def test_bybit_adapter_initialization(self):
        """Test Bybit adapter initialization"""
        adapter = BybitAdapter()

        assert adapter.rest.exchange_id == 'bybit'
        assert adapter.websocket.exchange_id == 'bybit'

        success = await adapter.initialize({})
        assert success is True

        status = await adapter.get_connection_status()
        assert status['exchange_id'] == 'bybit'

        await adapter.close()

    @pytest.mark.asyncio
    async def test_whitebit_adapter_initialization(self):
        """Test WhiteBit adapter initialization"""
        adapter = WhiteBitAdapter()

        assert adapter.rest.exchange_id == 'whitebit'
        assert adapter.websocket.exchange_id == 'whitebit'

        success = await adapter.initialize({})
        assert success is True

        status = await adapter.get_connection_status()
        assert status['exchange_id'] == 'whitebit'

        await adapter.close()

    @pytest.mark.asyncio
    async def test_exchange_factory(self):
        """Test exchange factory functionality"""
        # Test supported exchanges
        exchanges = ExchangeFactory.get_supported_exchanges()
        assert 'binance' in exchanges
        assert 'bybit' in exchanges
        assert 'whitebit' in exchanges

        # Test adapter creation
        binance_adapter = await ExchangeFactory.get_or_create_adapter('binance', use_cache=False)
        assert binance_adapter is not None
        assert binance_adapter.rest.exchange_id == 'binance'
        await binance_adapter.close()

        # Test unsupported exchange
        unsupported_adapter = await ExchangeFactory.get_or_create_adapter('unsupported', use_cache=False)
        assert unsupported_adapter is None

    @pytest.mark.asyncio
    async def test_exchange_manager(self):
        """Test exchange manager functionality"""
        manager = ExchangeManager()
        await manager.initialize()

        # Test getting public adapter
        adapter = await manager.get_public_adapter('binance')
        assert adapter is not None

        # Test exchange info
        info = await manager.get_exchange_info('binance')
        assert info is not None
        assert info['exchange_id'] == 'binance'
        assert 'markets' in info
        assert 'timeframes' in info

        # Test status
        status = await manager.get_all_exchanges_status()
        assert 'total_exchanges' in status
        assert 'exchanges' in status

        await manager.shutdown()

    @pytest.mock.patch('app.exchanges.base.ccxt')
    @pytest.mark.asyncio
    async def test_rest_adapter_with_mock(self, mock_ccxt):
        """Test REST adapter with mocked CCXT"""
        # Mock CCXT exchange
        mock_exchange = AsyncMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.timeframes = {'1m': '1m', '5m': '5m', '1h': '1h', '1d': '1d'}
        mock_exchange.markets = {'BTC/USDT': {'symbol': 'BTC/USDT'}}
        mock_exchange.fetch_ticker = AsyncMock(return_value={
            'symbol': 'BTC/USDT',
            'last': 50000,
            'bid': 49950,
            'ask': 50050
        })

        mock_ccxt.binance = MagicMock(return_value=mock_exchange)

        adapter = BinanceAdapter()
        success = await adapter.initialize({})
        assert success is True

        # Test ticker fetch
        ticker = await adapter.rest.fetch_ticker('BTC/USDT')
        assert ticker is not None
        assert ticker['symbol'] == 'BTC/USDT'
        assert ticker['last'] == 50000

        await adapter.close()

    @pytest.mark.asyncio
    async def test_websocket_subscription_flow(self):
        """Test WebSocket subscription flow"""
        adapter = BinanceAdapter()
        await adapter.initialize({})

        # Mock callback function
        callback_data = []

        async def test_callback(data):
            callback_data.append(data)

        # Test ticker subscription
        success = await adapter.websocket.subscribe_ticker('BTCUSDT', test_callback)
        assert success is True

        # Check subscriptions
        subscriptions = await adapter.websocket.get_subscriptions()
        assert subscriptions['total_subscriptions'] >= 1

        # Test unsubscribe
        success = await adapter.websocket.unsubscribe('ticker_BTCUSDT')
        assert success is True

        await adapter.close()

    def test_message_normalization(self):
        """Test WebSocket message normalization"""
        adapter = BinanceAdapter()

        # Test ticker normalization
        binance_ticker_msg = {
            'stream': 'btcusdt@ticker',
            'data': {
                's': 'BTCUSDT',
                'E': 1234567890000,
                'c': '50000',
                'o': '49000',
                'h': '51000',
                'l': '48000',
                'b': '49950',
                'a': '50050',
                'v': '100',
                'q': '5000000',
                'p': '1000',
                'P': '2.04'
            }
        }

        normalized = adapter.websocket._normalize_message(binance_ticker_msg)
        assert normalized is not None
        assert normalized['type'] == 'ticker'
        assert normalized['exchange'] == 'binance'
        assert normalized['symbol'] == 'BTCUSDT'
        assert normalized['data']['last'] == 50000.0
        assert normalized['data']['bid'] == 49950.0
        assert normalized['data']['ask'] == 50050.0

    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting functionality"""
        manager = ExchangeManager()
        await manager.initialize()

        # Test rate limit check
        assert manager._check_rate_limit('binance') is True

        # Simulate many requests
        for _ in range(1200):  # Binance limit is 1200
            manager._increment_rate_limit('binance')

        # Should hit rate limit
        assert manager._check_rate_limit('binance') is False

        # Reset time should allow requests again
        manager.rate_limit_monitors['binance']['last_reset'] = \
            manager.rate_limit_monitors['binance']['last_reset'].replace(minute=0)

        assert manager._check_rate_limit('binance') is True

        await manager.shutdown()

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in adapters"""
        adapter = BinanceAdapter()

        # Test safe_request with exception
        async def failing_function():
            raise Exception("Test error")

        result = await adapter.rest.safe_request(failing_function, default="default_value")
        assert result == "default_value"

        await adapter.close()


if __name__ == "__main__":
    pytest.main([__file__])

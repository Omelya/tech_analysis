import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import structlog
import redis
import aiomysql
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from historical_data_service import TaskPriority, historical_data_service
from exchanges.manager import exchange_manager
from config import settings
from models.database import HistoricalData, TradingPair, Exchange
from utils.rabbitmq import RabbitMQPublisher


class DataProcessor:
    """Automatic data processing and database updates"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="data_processor")
        self.redis_client: Optional[redis.Redis] = None
        self.db_engine = None
        self.session_factory = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None

        # Processing intervals (seconds)
        self.intervals = {
            'ticker_update': 5,  # Update tickers every 5 seconds
            'orderbook_update': 1,  # Update orderbooks every 1 second
            'klines_update': 60,  # Update 1m klines every minute
            'historical_sync': 300,  # Sync historical data every 5 minutes
        }

        # Active subscriptions and tasks
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.subscriptions: Dict[str, Dict] = {}

    async def initialize(self):
        """Initialize database connections and message broker"""
        try:
            # Initialize database
            self.db_engine = create_async_engine(settings.database_url)
            self.session_factory = sessionmaker(
                self.db_engine, class_=AsyncSession, expire_on_commit=False
            )

            # Initialize Redis
            self.redis_client = redis.from_url(
                f"redis://{settings.redis_host}:{settings.redis_port}/{settings.redis_db}"
            )

            # Initialize RabbitMQ publisher
            self.rabbitmq_publisher = RabbitMQPublisher(settings.rabbitmq_url)
            await self.rabbitmq_publisher.initialize()

            self.logger.info("Data processor initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize data processor", error=str(e))
            raise

    async def start_automatic_processing(self):
        """Start all automatic data processing tasks"""
        try:
            # Get active trading pairs from database
            active_pairs = await self.get_active_trading_pairs()

            for pair in active_pairs:
                exchange_slug = pair['exchange_slug']
                symbol = pair['symbol']

                # Start ticker updates
                task_key = f"ticker_{exchange_slug}_{symbol}"
                self.active_tasks[task_key] = asyncio.create_task(
                    self.ticker_update_loop(exchange_slug, symbol)
                )

                # Start klines updates for main timeframes
                for timeframe in ['1m', '5m', '15m', '1h', '4h', '1d']:
                    task_key = f"klines_{exchange_slug}_{symbol}_{timeframe}"
                    self.active_tasks[task_key] = asyncio.create_task(
                        self.klines_update_loop(exchange_slug, symbol, timeframe)
                    )

            # Start historical data sync
            self.active_tasks['historical_sync'] = asyncio.create_task(
                self.historical_sync_loop()
            )

            self.logger.info("All automatic processing tasks started",
                             active_tasks=len(self.active_tasks))

        except Exception as e:
            self.logger.error("Failed to start automatic processing", error=str(e))
            raise

    async def ticker_update_loop(self, exchange_slug: str, symbol: str):
        """Continuously update ticker data"""
        while True:
            try:
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    await asyncio.sleep(self.intervals['ticker_update'])
                    continue

                ticker = await adapter.rest.fetch_ticker(symbol)
                if ticker:
                    # Save to database
                    await self.save_ticker_to_db(exchange_slug, symbol, ticker)

                    # Cache in Redis
                    await self.cache_ticker_data(exchange_slug, symbol, ticker)

                    # Publish event to Laravel
                    await self.publish_ticker_update_event(exchange_slug, symbol, ticker)

                await asyncio.sleep(self.intervals['ticker_update'])

            except Exception as e:
                self.logger.error("Error in ticker update loop",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))
                await asyncio.sleep(self.intervals['ticker_update'])

    async def klines_update_loop(self, exchange_slug: str, symbol: str, timeframe: str):
        """Continuously update klines data"""
        while True:
            try:
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    await asyncio.sleep(self.intervals['klines_update'])
                    continue

                # Get latest candles (last 2 to ensure we have the current one)
                klines = await adapter.rest.fetch_ohlcv(symbol, timeframe, None, 2)
                if klines:
                    # Save to database
                    saved_count = await self.save_klines_to_db(exchange_slug, symbol, timeframe, klines)

                    if saved_count > 0:
                        # Publish event to Laravel
                        await self.publish_klines_update_event(exchange_slug, symbol, timeframe, klines)

                await asyncio.sleep(self.intervals['klines_update'])

            except Exception as e:
                self.logger.error("Error in klines update loop",
                                  exchange=exchange_slug, symbol=symbol,
                                  timeframe=timeframe, error=str(e))
                await asyncio.sleep(self.intervals['klines_update'])

    async def historical_sync_loop(self):
        """Periodically sync historical data gaps"""
        while True:
            try:
                active_pairs = await self.get_active_trading_pairs()

                for pair in active_pairs:
                    exchange_slug = pair['exchange_slug']
                    symbol = pair['symbol']

                    # Check for data gaps and fill them
                    await self.fill_historical_data_gaps(exchange_slug, symbol)

                await asyncio.sleep(self.intervals['historical_sync'])

            except Exception as e:
                self.logger.error("Error in historical sync loop", error=str(e))
                await asyncio.sleep(self.intervals['historical_sync'])

    async def save_ticker_to_db(self, exchange_slug: str, symbol: str, ticker: Dict[str, Any]):
        """Save ticker data to database"""
        async with self.session_factory() as session:
            try:
                # Update trading pair with latest ticker info
                query = """
                UPDATE trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                SET tp.last_price = :last_price,
                    tp.bid_price = :bid_price,
                    tp.ask_price = :ask_price,
                    tp.volume_24h = :volume_24h,
                    tp.price_change_24h = :price_change,
                    tp.price_change_percentage_24h = :price_change_percent,
                    tp.updated_at = NOW()
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                """

                await session.execute(query, {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol,
                    'last_price': ticker.get('last', 0),
                    'bid_price': ticker.get('bid', 0),
                    'ask_price': ticker.get('ask', 0),
                    'volume_24h': ticker.get('baseVolume', 0),
                    'price_change': ticker.get('change', 0),
                    'price_change_percent': ticker.get('percentage', 0)
                })

                await session.commit()

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to save ticker to DB",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))

    async def save_klines_to_db(self, exchange_slug: str, symbol: str,
                                timeframe: str, klines: List[List]) -> int:
        """Save klines data to database, return count of new records"""
        async with self.session_factory() as session:
            try:
                saved_count = 0

                # Get trading pair ID
                pair_query = """
                SELECT tp.id FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                """
                result = await session.execute(pair_query, {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol
                })
                pair_row = result.fetchone()

                if not pair_row:
                    return 0

                trading_pair_id = pair_row[0]

                # Insert new klines
                for kline in klines:
                    timestamp = datetime.fromtimestamp(kline[0] / 1000)

                    # Check if already exists
                    check_query = """
                    SELECT COUNT(*) FROM historical_data 
                    WHERE trading_pair_id = :pair_id 
                    AND timeframe = :timeframe 
                    AND timestamp = :timestamp
                    """
                    result = await session.execute(check_query, {
                        'pair_id': trading_pair_id,
                        'timeframe': timeframe,
                        'timestamp': timestamp
                    })

                    if result.scalar() == 0:
                        # Insert new record
                        insert_query = """
                        INSERT INTO historical_data 
                        (trading_pair_id, timeframe, timestamp, open, high, low, close, volume, created_at, updated_at)
                        VALUES (:pair_id, :timeframe, :timestamp, :open, :high, :low, :close, :volume, NOW(), NOW())
                        """

                        await session.execute(insert_query, {
                            'pair_id': trading_pair_id,
                            'timeframe': timeframe,
                            'timestamp': timestamp,
                            'open': kline[1],
                            'high': kline[2],
                            'low': kline[3],
                            'close': kline[4],
                            'volume': kline[5]
                        })
                        saved_count += 1

                await session.commit()
                return saved_count

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to save klines to DB",
                                  exchange=exchange_slug, symbol=symbol,
                                  timeframe=timeframe, error=str(e))
                return 0

    async def cache_ticker_data(self, exchange_slug: str, symbol: str, ticker: Dict[str, Any]):
        """Cache ticker data in Redis"""
        try:
            cache_key = f"ticker:{exchange_slug}:{symbol}"
            cache_data = {
                'data': ticker,
                'timestamp': datetime.now().isoformat(),
                'exchange': exchange_slug,
                'symbol': symbol
            }

            await self.redis_client.setex(
                cache_key,
                settings.cache_ttl_ticker,
                json.dumps(cache_data)
            )

        except Exception as e:
            self.logger.error("Failed to cache ticker data",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def publish_ticker_update_event(self, exchange_slug: str, symbol: str, ticker: Dict[str, Any]):
        """Publish ticker update event to Laravel via RabbitMQ"""
        try:
            event_data = {
                'event': 'crypto.price.updated',
                'exchange': exchange_slug,
                'symbol': symbol,
                'data': ticker,
                'timestamp': datetime.now().isoformat()
            }

            await self.rabbitmq_publisher.publish(
                exchange='crypto_events',
                routing_key='price.updated',
                message=event_data
            )

        except Exception as e:
            self.logger.error("Failed to publish ticker update event",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def publish_klines_update_event(self, exchange_slug: str, symbol: str,
                                          timeframe: str, klines: List[List]):
        """Publish klines update event to Laravel via RabbitMQ"""
        try:
            event_data = {
                'event': 'crypto.kline.updated',
                'exchange': exchange_slug,
                'symbol': symbol,
                'timeframe': timeframe,
                'data': klines,
                'timestamp': datetime.now().isoformat()
            }

            await self.rabbitmq_publisher.publish(
                exchange='crypto_events',
                routing_key='kline.updated',
                message=event_data
            )

        except Exception as e:
            self.logger.error("Failed to publish klines update event",
                              exchange=exchange_slug, symbol=symbol,
                              timeframe=timeframe, error=str(e))

    async def get_active_trading_pairs(self) -> List[Dict[str, Any]]:
        """Get list of active trading pairs from database"""
        async with self.session_factory() as session:
            try:
                query = """
                SELECT tp.symbol, e.slug as exchange_slug, tp.id as trading_pair_id
                FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE tp.is_active = 1 AND e.is_active = 1
                """

                result = await session.execute(query)
                return [{'symbol': row[0], 'exchange_slug': row[1], 'trading_pair_id': row[2]}
                        for row in result.fetchall()]

            except Exception as e:
                self.logger.error("Failed to get active trading pairs", error=str(e))
                return []

    async def stop_all_tasks(self):
        """Stop all processing tasks"""
        for task_name, task in self.active_tasks.items():
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error("Error stopping task", task=task_name, error=str(e))

        self.active_tasks.clear()

        if self.rabbitmq_publisher:
            await self.rabbitmq_publisher.close()

        if self.redis_client:
            await self.redis_client.close()

        self.logger.info("All processing tasks stopped")

    async def fill_historical_data_gaps(self, exchange_slug: str, symbol: str):
        """Fill missing historical data gaps"""
        try:
            self.logger.info("Checking for historical data gaps",
                             exchange=exchange_slug, symbol=symbol)

            # Get trading pair ID
            async with self.session_factory() as session:
                query = """
                SELECT tp.id FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                """

                result = await session.execute(query, {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol
                })

                pair_row = result.fetchone()
                if not pair_row:
                    self.logger.warning("Trading pair not found",
                                        exchange=exchange_slug, symbol=symbol)
                    return

                trading_pair_id = pair_row[0]

            # Check gaps for each timeframe
            for timeframe in settings.default_timeframes:
                # Get last timestamp for this pair/timeframe
                async with self.session_factory() as session:
                    query = """
                    SELECT 
                        MIN(timestamp) as first_time,
                        MAX(timestamp) as last_time,
                        COUNT(*) as record_count
                    FROM historical_data
                    WHERE trading_pair_id = :pair_id AND timeframe = :timeframe
                    """

                    result = await session.execute(query, {
                        'pair_id': trading_pair_id,
                        'timeframe': timeframe
                    })

                    stats_row = result.fetchone()
                    if not stats_row or stats_row[2] == 0:
                        # No data for this timeframe yet, schedule initial fetch
                        await historical_data_service.schedule_task(
                            exchange_slug=exchange_slug,
                            symbol=symbol,
                            timeframe=timeframe,
                            priority=TaskPriority.HIGH,
                            limit=1000
                        )
                        continue

                    first_time, last_time, record_count = stats_row

                    # Get expected record count based on timeframe
                    timeframe_seconds = self._get_timeframe_seconds(timeframe)
                    expected_records = int((last_time - first_time).total_seconds() / timeframe_seconds) + 1

                    if record_count < expected_records * 0.95:  # Allow 5% missing records
                        self.logger.info("Detected data gaps",
                                         exchange=exchange_slug, symbol=symbol,
                                         timeframe=timeframe,
                                         expected=expected_records,
                                         actual=record_count)

                        # Schedule gap detection and fill
                        await historical_data_service.detect_and_fill_gaps(
                            exchange_slug, symbol, timeframe
                        )

                    # Check if we need to update recent data
                    time_since_last = (datetime.now() - last_time).total_seconds()
                    if time_since_last > timeframe_seconds * 3:  # If last record is older than 3 intervals
                        self.logger.info("Scheduling recent data update",
                                         exchange=exchange_slug, symbol=symbol,
                                         timeframe=timeframe)

                        await historical_data_service.schedule_incremental_update(
                            exchange_slug, symbol, timeframe
                        )

            self.logger.info("Historical data gap check completed",
                             exchange=exchange_slug, symbol=symbol)

        except Exception as e:
            self.logger.error("Error checking for historical data gaps",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """Convert timeframe to seconds"""
        timeframe_map = {
            '1m': 60,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '4h': 14400,
            '1d': 86400
        }
        return timeframe_map.get(timeframe, 60)


# Global instance
data_processor = DataProcessor()

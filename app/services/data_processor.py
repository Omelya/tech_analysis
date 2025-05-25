import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import structlog
import redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from ..exchanges.manager import exchange_manager
from ..config import settings
from ..models.database import TradingPair, HistoricalData
from ..utils.rabbitmq import RabbitMQPublisher
from ..stream_processing.monitoring_system import monitoring_system


class DataProcessor:
    """Simplified data processor that works with stream processing system"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="data_processor")
        self.redis_client: Optional[redis.asyncio.Redis] = None
        self.db_engine = None
        self.session_factory = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None

        # Configuration
        self.intervals = {
            'ticker_update': 5,
            'klines_update': 60,
            'historical_sync': 300,
        }

        # Active tasks for background operations
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.subscriptions: Dict[str, Dict] = {}

        # Trading pairs cache
        self.active_trading_pairs: List[Dict[str, Any]] = []
        self.pairs_by_exchange: Dict[str, List[Dict[str, Any]]] = {}

    async def initialize(self):
        """Initialize database connections and message broker"""
        try:
            # Initialize database
            self.db_engine = create_async_engine(settings.database_url)
            self.session_factory = sessionmaker(
                self.db_engine, class_=AsyncSession, expire_on_commit=False
            )

            # Initialize Redis
            import redis.asyncio as redis_async
            self.redis_client = redis_async.from_url(
                f"redis://{settings.redis_host}:{settings.redis_port}/{settings.redis_db}",
                password=settings.redis_password if settings.redis_password else None,
                decode_responses=True,
            )

            # Initialize RabbitMQ publisher
            self.rabbitmq_publisher = RabbitMQPublisher(settings.rabbitmq_url)
            await self.rabbitmq_publisher.initialize()

            # Load trading pairs
            await self._load_trading_pairs()

            self.logger.info("Data processor initialized successfully",
                             total_pairs=len(self.active_trading_pairs))

        except Exception as e:
            self.logger.error("Failed to initialize data processor", error=str(e))
            raise

    async def _load_trading_pairs(self):
        """Load and organize trading pairs by exchange"""
        self.active_trading_pairs = await self.get_active_trading_pairs()

        # Group by exchange for efficient processing
        self.pairs_by_exchange.clear()
        for pair in self.active_trading_pairs:
            exchange_slug = pair['exchange_slug']
            if exchange_slug not in self.pairs_by_exchange:
                self.pairs_by_exchange[exchange_slug] = []
            self.pairs_by_exchange[exchange_slug].append(pair)

        self.logger.info("Trading pairs loaded",
                         exchanges=list(self.pairs_by_exchange.keys()),
                         pairs_per_exchange={k: len(v) for k, v in self.pairs_by_exchange.items()})

    async def start_automatic_processing(self):
        """Start lightweight background tasks (WebSocket integration handles most processing)"""
        try:
            # Only start essential background tasks
            # Most processing is now handled by stream_processing system

            # Historical data sync (reduced frequency)
            self.active_tasks['historical_sync'] = asyncio.create_task(
                self.historical_sync_loop()
            )

            # Trading pairs refresh task
            self.active_tasks['pairs_refresh'] = asyncio.create_task(
                self.pairs_refresh_loop()
            )

            self.logger.info("Background processing tasks started",
                             total_tasks=len(self.active_tasks))

        except Exception as e:
            self.logger.error("Failed to start automatic processing", error=str(e))
            raise

    async def historical_sync_loop(self):
        """Periodically sync historical data gaps (reduced load)"""
        while True:
            try:
                # Process historical sync in smaller batches with longer intervals
                batch_size = 20  # Reduced from 50

                for i in range(0, len(self.active_trading_pairs), batch_size):
                    batch = self.active_trading_pairs[i:i + batch_size]

                    for pair in batch:
                        exchange_slug = pair['exchange_slug']
                        symbol = pair['symbol']
                        await self.check_historical_gaps(exchange_slug, symbol)

                        # Longer delay between pairs
                        await asyncio.sleep(2)

                    # Longer delay between batches
                    await asyncio.sleep(30)

                # Much longer delay between full cycles
                await asyncio.sleep(self.intervals['historical_sync'] * 2)

            except Exception as e:
                self.logger.error("Error in historical sync loop", error=str(e))
                await asyncio.sleep(self.intervals['historical_sync'])

    async def pairs_refresh_loop(self):
        """Periodically refresh trading pairs"""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                await self.refresh_trading_pairs()
            except Exception as e:
                self.logger.error("Error in pairs refresh loop", error=str(e))
                await asyncio.sleep(300)

    async def check_historical_gaps(self, exchange_slug: str, symbol: str):
        """Check and fill missing historical data gaps - lightweight version"""
        try:
            # Get trading pair ID
            async with self.session_factory() as session:
                query = text("""
                SELECT tp.id FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                """)

                result = await session.execute(query, {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol
                })

                pair_row = result.fetchone()
                if not pair_row:
                    return

                trading_pair_id = pair_row[0]

            # Check only key timeframes to reduce load
            key_timeframes = ['1h', '1d']  # Only check hourly and daily

            for timeframe in key_timeframes:
                async with self.session_factory() as session:
                    query = text("""
                    SELECT 
                        MAX(timestamp) as last_time,
                        COUNT(*) as record_count
                    FROM historical_data
                    WHERE trading_pair_id = :pair_id AND timeframe = :timeframe
                    AND timestamp >= DATE_SUB(NOW(), INTERVAL 3 DAY)
                    """)

                    result = await session.execute(query, {
                        'pair_id': trading_pair_id,
                        'timeframe': timeframe
                    })

                    stats_row = result.fetchone()
                    if not stats_row or stats_row[1] == 0:
                        # Schedule fetch through stream processor if available
                        await self._schedule_historical_fetch(exchange_slug, symbol, timeframe)
                        continue

                    last_time, record_count = stats_row

                    # Only check if we're missing recent data (last 4 hours)
                    if last_time:
                        time_since_last = (datetime.now() - last_time).total_seconds()
                        timeframe_seconds = self._get_timeframe_seconds(timeframe)

                        if time_since_last > timeframe_seconds * 4:
                            await self._schedule_historical_fetch(exchange_slug, symbol, timeframe)

        except Exception as e:
            self.logger.error("Error checking historical gaps",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def _schedule_historical_fetch(self, exchange_slug: str, symbol: str, timeframe: str):
        """Schedule historical data fetch"""
        try:
            # Use exchange manager to fetch recent data
            adapter = await exchange_manager.get_public_adapter(exchange_slug)
            if not adapter:
                return

            # Fetch last 100 candles
            klines = await adapter.rest.fetch_ohlcv(symbol, timeframe, None, 100)
            if klines:
                await self.save_klines_to_db(exchange_slug, symbol, timeframe, klines)
                self.logger.debug("Fetched historical data",
                                  exchange=exchange_slug, symbol=symbol,
                                  timeframe=timeframe, count=len(klines))

        except Exception as e:
            self.logger.error("Failed to schedule historical fetch",
                              exchange=exchange_slug, symbol=symbol,
                              timeframe=timeframe, error=str(e))

    async def save_klines_to_db(self, exchange_slug: str, symbol: str,
                                timeframe: str, klines: List[List]) -> int:
        """Save klines data to database"""
        async with self.session_factory() as session:
            try:
                trading_pair = await TradingPair.get_by_exchange_and_symbol(session, exchange_slug, symbol)
                if not trading_pair:
                    return 0

                saved_count = await HistoricalData.save_klines(session, trading_pair.id, timeframe, klines)

                # Record metrics
                monitoring_system.record_histogram('database_write_duration', 0.1,
                                                   {'operation': 'save_klines'})
                monitoring_system.increment_counter('database_operations_total',
                                                    {'operation': 'klines_save', 'status': 'success'})

                return saved_count

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to save klines to DB",
                                  exchange=exchange_slug, symbol=symbol,
                                  timeframe=timeframe, error=str(e))

                # Record error metrics
                monitoring_system.increment_counter('database_operations_total',
                                                    {'operation': 'klines_save', 'status': 'error'})
                return 0

    async def cache_data(self, key: str, data: Dict[str, Any], ttl: int = None):
        """Cache data in Redis"""
        try:
            cache_data = {
                'data': data,
                'timestamp': datetime.now().isoformat()
            }

            ttl = ttl or settings.cache_ttl_ticker
            await self.redis_client.set(
                key,
                json.dumps(cache_data, default=str),
                ex=ttl
            )

        except Exception as e:
            self.logger.error("Failed to cache data", key=key, error=str(e))

    async def get_cached_data(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached data from Redis"""
        try:
            cached_data = await self.redis_client.get(key)
            if cached_data:
                return json.loads(cached_data)
            return None

        except Exception as e:
            self.logger.error("Failed to get cached data", key=key, error=str(e))
            return None

    async def publish_event(self, event_type: str, exchange: str, symbol: str, data: Dict[str, Any]):
        """Publish event to Laravel via RabbitMQ"""
        try:
            event_data = {
                'event': event_type,
                'exchange': exchange,
                'symbol': symbol,
                'data': data,
                'timestamp': datetime.now().isoformat()
            }

            routing_key = event_type.split('.')[-1]  # Extract last part as routing key

            await self.rabbitmq_publisher.publish(
                exchange='crypto_events',
                routing_key=routing_key,
                message=event_data
            )

        except Exception as e:
            self.logger.error("Failed to publish event",
                              event_type=event_type, exchange=exchange,
                              symbol=symbol, error=str(e))

    async def get_active_trading_pairs(self) -> List[Dict[str, Any]]:
        """Get list of active trading pairs from database"""
        async with self.session_factory() as session:
            try:
                return await TradingPair.get_active_pairs(session)
            except Exception as e:
                self.logger.error("Failed to get active trading pairs", error=str(e))
                return []

    async def refresh_trading_pairs(self):
        """Refresh trading pairs data"""
        try:
            old_count = len(self.active_trading_pairs)
            await self._load_trading_pairs()
            new_count = len(self.active_trading_pairs)

            self.logger.info("Trading pairs refreshed",
                             old_count=old_count, new_count=new_count)

        except Exception as e:
            self.logger.error("Failed to refresh trading pairs", error=str(e))

    async def stop_all_tasks(self):
        """Stop all processing tasks"""
        self.logger.info("Stopping all processing tasks", task_count=len(self.active_tasks))

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

        if self.db_engine:
            await self.db_engine.dispose()

        self.logger.info("All processing tasks stopped")

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

    def get_processor_stats(self) -> Dict[str, Any]:
        """Get processor statistics"""
        return {
            'active_tasks': len(self.active_tasks),
            'active_trading_pairs': len(self.active_trading_pairs),
            'pairs_by_exchange': {k: len(v) for k, v in self.pairs_by_exchange.items()},
            'task_types': list(self.active_tasks.keys()),
            'redis_connected': self.redis_client is not None,
            'rabbitmq_connected': self.rabbitmq_publisher is not None
        }

    # Integration methods for stream processing
    async def handle_stream_data(self, exchange: str, symbol: str, message_type: str, data: Dict[str, Any]):
        """Handle data from stream processing system"""
        try:
            # Cache the data
            cache_key = f"{message_type}:{exchange}:{symbol}"
            await self.cache_data(cache_key, data)

            # Publish to Laravel
            event_type = f"crypto.{message_type}.updated"
            await self.publish_event(event_type, exchange, symbol, data)

            # Update database if needed (for tickers)
            if message_type == 'ticker':
                await self._update_ticker_in_db(exchange, symbol, data)

        except Exception as e:
            self.logger.error("Error handling stream data",
                              exchange=exchange, symbol=symbol,
                              message_type=message_type, error=str(e))

    async def _update_ticker_in_db(self, exchange: str, symbol: str, ticker_data: Dict[str, Any]):
        """Update ticker data in database"""
        async with self.session_factory() as session:
            try:
                success = await TradingPair.update_ticker_data(session, exchange, symbol, ticker_data)
                if success:
                    monitoring_system.increment_counter('database_operations_total',
                                                        {'operation': 'ticker_update', 'status': 'success'})
            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to update ticker in DB",
                                  exchange=exchange, symbol=symbol, error=str(e))
                monitoring_system.increment_counter('database_operations_total',
                                                    {'operation': 'ticker_update', 'status': 'error'})


# Global instance
data_processor = DataProcessor()

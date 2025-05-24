import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import structlog
import redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from .historical_data_service import historical_data_service, TaskPriority
from ..exchanges.manager import exchange_manager
from ..config import settings
from ..models.database import TradingPair, HistoricalData
from ..utils.rabbitmq import RabbitMQPublisher


class DataProcessor:
    """Optimized automatic data processing with shared adapters"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="data_processor")
        self.redis_client: Optional[redis.asyncio.Redis] = None
        self.db_engine = None
        self.session_factory = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None

        # Processing intervals (seconds)
        self.intervals = {
            'ticker_update': 5,
            'orderbook_update': 1,
            'klines_update': 60,
            'historical_sync': 300,
        }

        # Task management - OPTIMIZED
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.subscriptions: Dict[str, Dict] = {}

        # Shared data structures to reduce adapter creation
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
        """Start optimized data processing tasks"""
        try:
            for exchange_slug, pairs in self.pairs_by_exchange.items():
                # One ticker task per exchange (handles all pairs)
                task_key = f"ticker_batch_{exchange_slug}"
                self.active_tasks[task_key] = asyncio.create_task(
                    self.ticker_batch_update_loop(exchange_slug, pairs)
                )

                # One klines task per exchange per timeframe
                for timeframe in ['1m', '5m', '15m', '1h', '4h', '1d']:
                    task_key = f"klines_batch_{exchange_slug}_{timeframe}"
                    self.active_tasks[task_key] = asyncio.create_task(
                        self.klines_batch_update_loop(exchange_slug, pairs, timeframe)
                    )

            # Historical data sync (global)
            self.active_tasks['historical_sync'] = asyncio.create_task(
                self.historical_sync_loop()
            )

            self.logger.info("Optimized processing tasks started",
                             total_tasks=len(self.active_tasks),
                             instead_of_individual_tasks=len(self.active_trading_pairs) * 7)

        except Exception as e:
            self.logger.error("Failed to start automatic processing", error=str(e))
            raise

    async def ticker_batch_update_loop(self, exchange_slug: str, pairs: List[Dict[str, Any]]):
        """Update tickers for all pairs of an exchange in batches"""
        while True:
            try:
                # Get single shared adapter for this exchange
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    self.logger.warning("No adapter available", exchange=exchange_slug)
                    await asyncio.sleep(self.intervals['ticker_update'])
                    continue

                # Process pairs in batches to avoid overwhelming the API
                batch_size = 20  # Adjust based on exchange limits

                for i in range(0, len(pairs), batch_size):
                    batch = pairs[i:i + batch_size]

                    # Process batch concurrently but with the same adapter
                    tasks = []
                    for pair in batch:
                        task = self._process_single_ticker(adapter, exchange_slug, pair['symbol'])
                        tasks.append(task)

                    # Wait for batch to complete
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Small delay between batches to respect rate limits
                    await asyncio.sleep(0.1)

                await asyncio.sleep(self.intervals['ticker_update'])

            except Exception as e:
                self.logger.error("Error in ticker batch update loop",
                                  exchange=exchange_slug, error=str(e))
                await asyncio.sleep(self.intervals['ticker_update'])

    async def _process_single_ticker(self, adapter, exchange_slug: str, symbol: str):
        """Process single ticker with shared adapter"""
        try:
            ticker = await adapter.rest.fetch_ticker(symbol)
            if ticker:
                await self.save_ticker_to_db(exchange_slug, symbol, ticker)
                await self.cache_ticker_data(exchange_slug, symbol, ticker)
                await self.publish_ticker_update_event(exchange_slug, symbol, ticker)
        except Exception as e:
            # Log but don't fail the entire batch
            self.logger.debug("Failed to process ticker",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def klines_batch_update_loop(self, exchange_slug: str, pairs: List[Dict[str, Any]], timeframe: str):
        """Update klines for all pairs of an exchange and timeframe"""
        while True:
            try:
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    await asyncio.sleep(self.intervals['klines_update'])
                    continue

                # Process pairs in smaller batches for klines (more intensive)
                batch_size = 10

                for i in range(0, len(pairs), batch_size):
                    batch = pairs[i:i + batch_size]

                    tasks = []
                    for pair in batch:
                        task = self._process_single_klines(adapter, exchange_slug, pair['symbol'], timeframe)
                        tasks.append(task)

                    await asyncio.gather(*tasks, return_exceptions=True)
                    await asyncio.sleep(0.2)  # Longer delay for klines

                await asyncio.sleep(self.intervals['klines_update'])

            except Exception as e:
                self.logger.error("Error in klines batch update loop",
                                  exchange=exchange_slug, timeframe=timeframe, error=str(e))
                await asyncio.sleep(self.intervals['klines_update'])

    async def _process_single_klines(self, adapter, exchange_slug: str, symbol: str, timeframe: str):
        """Process single klines with shared adapter"""
        try:
            klines = await adapter.rest.fetch_ohlcv(symbol, timeframe, None, 2)
            if klines:
                saved_count = await self.save_klines_to_db(exchange_slug, symbol, timeframe, klines)
                if saved_count > 0:
                    await self.publish_klines_update_event(exchange_slug, symbol, timeframe, klines)
        except Exception as e:
            self.logger.debug("Failed to process klines",
                              exchange=exchange_slug, symbol=symbol, timeframe=timeframe, error=str(e))

    async def historical_sync_loop(self):
        """Periodically sync historical data gaps"""
        while True:
            try:
                # Process historical sync in smaller batches
                batch_size = 50  # Only 50 pairs at a time

                for i in range(0, len(self.active_trading_pairs), batch_size):
                    batch = self.active_trading_pairs[i:i + batch_size]

                    for pair in batch:
                        exchange_slug = pair['exchange_slug']
                        symbol = pair['symbol']
                        await self.fill_historical_data_gaps(exchange_slug, symbol)

                        # Small delay between pairs
                        await asyncio.sleep(1)

                    # Longer delay between batches
                    await asyncio.sleep(10)

                await asyncio.sleep(self.intervals['historical_sync'])

            except Exception as e:
                self.logger.error("Error in historical sync loop", error=str(e))
                await asyncio.sleep(self.intervals['historical_sync'])

    async def save_ticker_to_db(self, exchange_slug: str, symbol: str, ticker: Dict[str, Any]):
        """Save ticker data to database"""
        async with self.session_factory() as session:
            try:
                await TradingPair.update_ticker_data(session, exchange_slug, symbol, ticker)
            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to save ticker to DB",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))

    async def save_klines_to_db(self, exchange_slug: str, symbol: str,
                                timeframe: str, klines: List[List]) -> int:
        """Save klines data to database"""
        async with self.session_factory() as session:
            try:
                trading_pair = await TradingPair.get_by_exchange_and_symbol(session, exchange_slug, symbol)
                if not trading_pair:
                    return 0

                return await HistoricalData.save_klines(session, trading_pair.id, timeframe, klines)

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

            await self.redis_client.set(
                    cache_key,
                    json.dumps(cache_data),
                    ex=settings.cache_ttl_ticker
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
                return await TradingPair.get_active_pairs(session)
            except Exception as e:
                self.logger.error("Failed to get active trading pairs", error=str(e))
                return []

    async def refresh_trading_pairs(self):
        """Refresh trading pairs data (call when pairs are added/removed)"""
        try:
            old_count = len(self.active_trading_pairs)
            await self._load_trading_pairs()
            new_count = len(self.active_trading_pairs)

            self.logger.info("Trading pairs refreshed",
                             old_count=old_count, new_count=new_count)

            # If significant changes, consider restarting processing tasks
            if abs(new_count - old_count) > 100:
                self.logger.info("Significant changes detected, consider restarting processor")

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

    async def fill_historical_data_gaps(self, exchange_slug: str, symbol: str):
        """Fill missing historical data gaps - optimized version"""
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

            # Check only main timeframes to reduce load
            main_timeframes = ['1h', '4h', '1d']  # Reduced from all timeframes

            for timeframe in main_timeframes:
                async with self.session_factory() as session:
                    query = text("""
                    SELECT 
                        MAX(timestamp) as last_time,
                        COUNT(*) as record_count
                    FROM historical_data
                    WHERE trading_pair_id = :pair_id AND timeframe = :timeframe
                    AND timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                    """)

                    result = await session.execute(query, {
                        'pair_id': trading_pair_id,
                        'timeframe': timeframe
                    })

                    stats_row = result.fetchone()
                    if not stats_row or stats_row[1] == 0:
                        task_id = historical_data_service.schedule_optimized_task(
                            exchange_slug=exchange_slug,
                            symbol=symbol,
                            timeframe=timeframe,
                            priority=TaskPriority.MEDIUM,  # Reduced priority
                            limit=168  # 1 week of hourly data
                        )
                        self.logger.info(
                            f"Scheduled initial fetch for {exchange_slug}:{symbol}:{timeframe}, task_id: {task_id}")
                        continue

                    last_time, record_count = stats_row

                    # Only check if we're missing recent data (last 2 hours)
                    if last_time:
                        time_since_last = (datetime.now() - last_time).total_seconds()
                        timeframe_seconds = self._get_timeframe_seconds(timeframe)

                        if time_since_last > timeframe_seconds * 2:
                            if asyncio.iscoroutinefunction(historical_data_service.schedule_incremental_update):
                                await historical_data_service.schedule_incremental_update(
                                    exchange_slug, symbol, timeframe
                                )
                            else:
                                historical_data_service.schedule_incremental_update(
                                    exchange_slug, symbol, timeframe
                                )

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

    def get_processor_stats(self) -> Dict[str, Any]:
        """Get processor statistics"""
        return {
            'active_tasks': len(self.active_tasks),
            'active_trading_pairs': len(self.active_trading_pairs),
            'pairs_by_exchange': {k: len(v) for k, v in self.pairs_by_exchange.items()},
            'task_types': list(set(task_name.split('_')[0] for task_name in self.active_tasks.keys())),
            'optimization_ratio': f"Using {len(self.active_tasks)} tasks instead of {len(self.active_trading_pairs) * 7}"
        }


# Global instance
data_processor = DataProcessor()

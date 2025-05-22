import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import heapq
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from ..exchanges.manager import exchange_manager
from ..config import settings
from ..utils.metrics_collector import metrics_collector
from ..utils.db_error_handler import db_error_handler


class TaskPriority(Enum):
    """Task priority levels"""
    LOW = 3
    MEDIUM = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class HistoricalDataTask:
    """Historical data fetch task"""
    priority: TaskPriority
    exchange_slug: str
    symbol: str
    timeframe: str
    since: Optional[int]
    limit: int
    created_at: float
    retries: int = 0
    max_retries: int = 3

    def __lt__(self, other):
        """Compare tasks for priority queue"""
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.created_at < other.created_at


class HistoricalDataService:
    """Service for fetching and managing historical data"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="historical_data_service")
        self.task_queue: List[HistoricalDataTask] = []
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.running = False

        # Rate limiting per exchange
        self.rate_limits = {
            'binance': {'requests_per_minute': 1200, 'current': 0, 'reset_time': 0},
            'bybit': {'requests_per_minute': 600, 'current': 0, 'reset_time': 0},
            'whitebit': {'requests_per_minute': 300, 'current': 0, 'reset_time': 0}
        }

        # Session factory
        self.session_factory = None

        # Task processing settings
        self.max_concurrent_tasks = 5
        self.batch_size = 1000

    async def initialize(self, session_factory):
        """Initialize the service"""
        self.session_factory = session_factory
        self.running = True

        # Start task processor
        self.active_tasks['processor'] = asyncio.create_task(self._process_tasks())
        self.active_tasks['rate_limiter'] = asyncio.create_task(self._rate_limit_reset_loop())

        self.logger.info("Historical data service initialized")

    async def shutdown(self):
        """Shutdown the service"""
        self.running = False

        # Cancel all tasks
        for task in self.active_tasks.values():
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self.active_tasks.values(), return_exceptions=True)
        self.active_tasks.clear()

        self.logger.info("Historical data service shutdown")

    def schedule_task(self, exchange_slug: str, symbol: str, timeframe: str,
                      priority: TaskPriority = TaskPriority.MEDIUM,
                      since: Optional[int] = None, limit: int = 1000) -> str:
        """Schedule a historical data fetch task"""
        task = HistoricalDataTask(
            priority=priority,
            exchange_slug=exchange_slug,
            symbol=symbol,
            timeframe=timeframe,
            since=since,
            limit=limit,
            created_at=time.time()
        )

        heapq.heappush(self.task_queue, task)

        task_id = f"{exchange_slug}_{symbol}_{timeframe}_{task.created_at}"
        self.logger.info("Task scheduled", task_id=task_id, priority=priority.name)

        metrics_collector.increment_counter('historical_tasks_scheduled',
                                            {'exchange': exchange_slug, 'priority': priority.name})

        return task_id

    def schedule_bulk_import(self, exchange_slug: str, symbol: str,
                             timeframes: List[str], days_back: int = 30,
                             priority: TaskPriority = TaskPriority.HIGH) -> List[str]:
        """Schedule bulk historical data import"""
        task_ids = []
        base_time = datetime.now()

        for timeframe in timeframes:
            # Calculate since timestamp based on timeframe and days_back
            since = int((base_time - timedelta(days=days_back)).timestamp() * 1000)

            task_id = self.schedule_task(
                exchange_slug=exchange_slug,
                symbol=symbol,
                timeframe=timeframe,
                priority=priority,
                since=since,
                limit=self.batch_size
            )
            task_ids.append(task_id)

        self.logger.info("Bulk import scheduled", exchange=exchange_slug,
                         symbol=symbol, timeframes=timeframes, tasks=len(task_ids))

        return task_ids

    async def schedule_incremental_update(self, exchange_slug: str, symbol: str, timeframe: str) -> Optional[str]:
        """Schedule incremental update for recent data"""
        try:
            # Get last timestamp for this pair/timeframe
            async with self.session_factory() as session:
                query = """
                SELECT MAX(hd.timestamp) as latest_time
                FROM historical_data hd
                JOIN trading_pairs tp ON hd.trading_pair_id = tp.id
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol AND hd.timeframe = :timeframe
                """

                result = await session.execute(text(query), {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol,
                    'timeframe': timeframe
                })

                latest_row = result.fetchone()
                latest_time = latest_row[0] if latest_row and latest_row[0] else None

            # Schedule update from last known time
            since = int(latest_time.timestamp() * 1000) if latest_time else None

            return self.schedule_task(
                exchange_slug=exchange_slug,
                symbol=symbol,
                timeframe=timeframe,
                priority=TaskPriority.HIGH,
                since=since,
                limit=500
            )

        except Exception as e:
            self.logger.error("Error scheduling incremental update",
                              exchange=exchange_slug, symbol=symbol, timeframe=timeframe, error=str(e))
            return None

    async def detect_and_fill_gaps(self, exchange_slug: str, symbol: str, timeframe: str) -> List[str]:
        """Detect data gaps and schedule fill tasks"""
        try:
            gaps = await self._detect_data_gaps(exchange_slug, symbol, timeframe)
            task_ids = []

            for gap_start, gap_end in gaps:
                task_id = self.schedule_task(
                    exchange_slug=exchange_slug,
                    symbol=symbol,
                    timeframe=timeframe,
                    priority=TaskPriority.MEDIUM,
                    since=int(gap_start.timestamp() * 1000),
                    limit=min(1000, int((gap_end - gap_start).total_seconds() / self._get_timeframe_seconds(timeframe)))
                )
                task_ids.append(task_id)

            if gaps:
                self.logger.info("Data gaps detected and scheduled",
                                 exchange=exchange_slug, symbol=symbol, timeframe=timeframe, gaps=len(gaps))

            return task_ids

        except Exception as e:
            self.logger.error("Error detecting gaps",
                              exchange=exchange_slug, symbol=symbol, timeframe=timeframe, error=str(e))
            return []

    async def _detect_data_gaps(self, exchange_slug: str, symbol: str, timeframe: str) -> List[
        Tuple[datetime, datetime]]:
        """Detect gaps in historical data"""
        try:
            async with self.session_factory() as session:
                query = """
                SELECT hd.timestamp
                FROM historical_data hd
                JOIN trading_pairs tp ON hd.trading_pair_id = tp.id
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol AND hd.timeframe = :timeframe
                AND hd.timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY hd.timestamp ASC
                """

                result = await session.execute(text(query), {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol,
                    'timeframe': timeframe
                })

                timestamps = [row[0] for row in result.fetchall()]

            if len(timestamps) < 2:
                return []

            # Detect gaps
            gaps = []
            timeframe_delta = timedelta(seconds=self._get_timeframe_seconds(timeframe))

            for i in range(1, len(timestamps)):
                expected_time = timestamps[i - 1] + timeframe_delta
                actual_time = timestamps[i]

                # If gap is larger than 2 intervals, consider it a gap
                if actual_time - expected_time > timeframe_delta * 2:
                    gaps.append((expected_time, actual_time))

            return gaps

        except Exception as e:
            self.logger.error("Error detecting data gaps", error=str(e))
            return []

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

    async def _process_tasks(self):
        """Main task processing loop"""
        active_workers = []

        while self.running:
            try:
                # Clean up completed workers
                active_workers = [w for w in active_workers if not w.done()]

                # Process tasks if we have capacity and tasks available
                while len(active_workers) < self.max_concurrent_tasks and self.task_queue:
                    if not self._can_make_request(self.task_queue[0].exchange_slug):
                        break

                    task = heapq.heappop(self.task_queue)
                    worker = asyncio.create_task(self._execute_task(task))
                    active_workers.append(worker)

                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error("Error in task processing loop", error=str(e))
                await asyncio.sleep(5)

    async def _execute_task(self, task: HistoricalDataTask):
        """Execute a historical data fetch task"""
        start_time = time.time()

        try:
            self.logger.info("Executing task",
                             exchange=task.exchange_slug, symbol=task.symbol,
                             timeframe=task.timeframe, priority=task.priority.name)

            # Get adapter
            adapter = await exchange_manager.get_public_adapter(task.exchange_slug)
            if not adapter:
                raise Exception(f"Could not get adapter for {task.exchange_slug}")

            # Fetch data
            klines = await adapter.rest.fetch_ohlcv(
                task.symbol, task.timeframe, task.since, task.limit
            )

            if not klines:
                self.logger.warning("No data received",
                                    exchange=task.exchange_slug, symbol=task.symbol, timeframe=task.timeframe)
                return

            # Save data
            saved_count = await self._save_historical_data(task, klines)

            # Update rate limit
            self._increment_rate_limit(task.exchange_slug)

            # Record metrics
            execution_time = time.time() - start_time
            metrics_collector.record_histogram('historical_task_duration', execution_time,
                                               {'exchange': task.exchange_slug, 'timeframe': task.timeframe})
            metrics_collector.record_data_processing('historical', task.exchange_slug, task.symbol,
                                                     saved_count, execution_time)

            self.logger.info("Task completed",
                             exchange=task.exchange_slug, symbol=task.symbol,
                             timeframe=task.timeframe, saved=saved_count, duration=execution_time)

        except Exception as e:
            self.logger.error("Task execution failed",
                              exchange=task.exchange_slug, symbol=task.symbol,
                              timeframe=task.timeframe, error=str(e), retries=task.retries)

            # Retry logic
            if task.retries < task.max_retries:
                task.retries += 1
                task.created_at = time.time() + (task.retries * 30)  # Exponential backoff
                heapq.heappush(self.task_queue, task)
                self.logger.info("Task rescheduled for retry", retries=task.retries)
            else:
                metrics_collector.increment_counter('historical_task_failures',
                                                    {'exchange': task.exchange_slug, 'reason': 'max_retries'})

    async def _save_historical_data(self, task: HistoricalDataTask, klines: List[List]) -> int:
        """Save historical data to database"""
        async with self.session_factory() as session:
            try:
                return await db_error_handler.execute_with_retry(
                    session, self._save_klines_operation, task.exchange_slug, task.symbol, task.timeframe, klines
                )
            except Exception as e:
                self.logger.error("Failed to save historical data after retries",
                                  exchange=task.exchange_slug, symbol=task.symbol, timeframe=task.timeframe,
                                  error=str(e))
                return 0

    async def _save_klines_operation(self, session: AsyncSession, exchange_slug: str, symbol: str,
                                     timeframe: str, klines: List[List]) -> int:
        """Database operation for saving klines"""
        saved_count = 0

        # Get trading pair ID
        pair_query = """
        SELECT tp.id FROM trading_pairs tp
        JOIN exchanges e ON tp.exchange_id = e.id
        WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
        """
        result = await session.execute(text(pair_query), {
            'exchange_slug': exchange_slug,
            'symbol': symbol
        })
        pair_row = result.fetchone()

        if not pair_row:
            return 0

        trading_pair_id = pair_row[0]

        # Batch insert for better performance
        insert_values = []
        for kline in klines:
            timestamp = datetime.fromtimestamp(kline[0] / 1000)

            # Check if already exists (only for critical data)
            check_query = """
            SELECT COUNT(*) FROM historical_data 
            WHERE trading_pair_id = :pair_id 
            AND timeframe = :timeframe 
            AND timestamp = :timestamp
            """
            result = await session.execute(text(check_query), {
                'pair_id': trading_pair_id,
                'timeframe': timeframe,
                'timestamp': timestamp
            })

            if result.scalar() == 0:
                insert_values.append({
                    'pair_id': trading_pair_id,
                    'timeframe': timeframe,
                    'timestamp': timestamp,
                    'open': kline[1],
                    'high': kline[2],
                    'low': kline[3],
                    'close': kline[4],
                    'volume': kline[5]
                })

        # Batch insert
        if insert_values:
            insert_query = """
            INSERT INTO historical_data 
            (trading_pair_id, timeframe, timestamp, open, high, low, close, volume, created_at, updated_at)
            VALUES (:pair_id, :timeframe, :timestamp, :open, :high, :low, :close, :volume, NOW(), NOW())
            """

            await session.execute(text(insert_query), insert_values)
            saved_count = len(insert_values)

        await session.commit()
        return saved_count

    def _can_make_request(self, exchange_slug: str) -> bool:
        """Check if we can make a request to the exchange"""
        if exchange_slug not in self.rate_limits:
            return True

        limit_info = self.rate_limits[exchange_slug]
        current_time = time.time()

        # Reset counter if minute has passed
        if current_time - limit_info['reset_time'] >= 60:
            limit_info['current'] = 0
            limit_info['reset_time'] = current_time

        return limit_info['current'] < limit_info['requests_per_minute']

    def _increment_rate_limit(self, exchange_slug: str):
        """Increment rate limit counter"""
        if exchange_slug in self.rate_limits:
            self.rate_limits[exchange_slug]['current'] += 1

    async def _rate_limit_reset_loop(self):
        """Reset rate limits periodically"""
        while self.running:
            try:
                await asyncio.sleep(60)
                current_time = time.time()

                for exchange_slug, limit_info in self.rate_limits.items():
                    if current_time - limit_info['reset_time'] >= 60:
                        limit_info['current'] = 0
                        limit_info['reset_time'] = current_time

            except Exception as e:
                self.logger.error("Error in rate limit reset loop", error=str(e))

    def get_service_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            'running': self.running,
            'queue_size': len(self.task_queue),
            'active_workers': len([t for t in self.active_tasks.values() if not t.done()]),
            'rate_limits': {
                exchange: {
                    'current': info['current'],
                    'limit': info['requests_per_minute'],
                    'remaining': info['requests_per_minute'] - info['current']
                }
                for exchange, info in self.rate_limits.items()
            }
        }

    async def validate_data_integrity(self, exchange_slug: str, symbol: str, timeframe: str,
                                      days_back: int = 7) -> Dict[str, Any]:
        """Validate data integrity for a trading pair"""
        try:
            async with self.session_factory() as session:
                # Check for missing data in the last N days
                query = """
                SELECT 
                    DATE(hd.timestamp) as date,
                    COUNT(*) as count,
                    MIN(hd.timestamp) as first_timestamp,
                    MAX(hd.timestamp) as last_timestamp
                FROM historical_data hd
                JOIN trading_pairs tp ON hd.trading_pair_id = tp.id
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange_slug 
                AND tp.symbol = :symbol 
                AND hd.timeframe = :timeframe
                AND hd.timestamp >= DATE_SUB(NOW(), INTERVAL :days_back DAY)
                GROUP BY DATE(hd.timestamp)
                ORDER BY date DESC
                """

                result = await session.execute(text(query), {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'days_back': days_back
                })

                daily_stats = []
                for row in result.fetchall():
                    daily_stats.append({
                        'date': row[0].isoformat(),
                        'count': row[1],
                        'first_timestamp': row[2].isoformat(),
                        'last_timestamp': row[3].isoformat()
                    })

            # Calculate expected counts per day
            expected_per_day = 86400 // self._get_timeframe_seconds(timeframe)

            # Analyze data quality
            missing_days = []
            incomplete_days = []

            for stats in daily_stats:
                if stats['count'] == 0:
                    missing_days.append(stats['date'])
                elif stats['count'] < expected_per_day * 0.9:  # Less than 90% of expected data
                    incomplete_days.append({
                        'date': stats['date'],
                        'count': stats['count'],
                        'expected': expected_per_day,
                        'percentage': (stats['count'] / expected_per_day) * 100
                    })

            return {
                'exchange': exchange_slug,
                'symbol': symbol,
                'timeframe': timeframe,
                'days_analyzed': days_back,
                'total_days_with_data': len(daily_stats),
                'missing_days': missing_days,
                'incomplete_days': incomplete_days,
                'data_quality_score': max(0, 100 - len(missing_days) * 10 - len(incomplete_days) * 5),
                'daily_stats': daily_stats[:7]  # Last 7 days
            }

        except Exception as e:
            self.logger.error("Error validating data integrity",
                              exchange=exchange_slug, symbol=symbol, timeframe=timeframe, error=str(e))
            return {}


# Global instance
historical_data_service = HistoricalDataService()

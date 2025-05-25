import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import structlog

from ..exchanges.manager import exchange_manager
from ..models.database import TradingPair, HistoricalData
from ..stream_processing.monitoring_system import monitoring_system


class TaskPriority(Enum):
    """Task priority levels"""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class HistoricalTask:
    """Historical data task"""
    exchange_slug: str
    symbol: str
    timeframe: str
    priority: TaskPriority
    since: Optional[int] = None
    limit: int = 1000
    created_at: float = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


class HistoricalDataService:
    """Simplified historical data service"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="historical_service")
        self.session_factory = None
        self.running = False

        # Task management
        self.pending_tasks: Dict[str, HistoricalTask] = {}
        self.active_tasks: Dict[str, asyncio.Task] = {}

        # Performance tracking
        self.completion_stats: Dict[str, List[float]] = {}
        self.error_rates: Dict[str, float] = {}

    async def initialize(self, session_factory):
        """Initialize service"""
        self.session_factory = session_factory
        self.running = True

        # Start task processor
        asyncio.create_task(self._task_processor_loop())

        self.logger.info("Historical data service initialized")

    async def shutdown(self):
        """Shutdown service"""
        self.running = False

        # Cancel active tasks
        for task in self.active_tasks.values():
            task.cancel()

        self.logger.info("Historical data service shutdown complete")

    def schedule_task(self, exchange_slug: str, symbol: str, timeframe: str,
                      priority: TaskPriority = TaskPriority.MEDIUM,
                      since: Optional[int] = None, limit: int = 1000) -> str:
        """Schedule historical data task"""

        task = HistoricalTask(
            exchange_slug=exchange_slug,
            symbol=symbol,
            timeframe=timeframe,
            priority=priority,
            since=since,
            limit=limit
        )

        task_id = f"{exchange_slug}_{symbol}_{timeframe}_{int(task.created_at)}"
        self.pending_tasks[task_id] = task

        self.logger.info("Task scheduled", task_id=task_id, priority=priority.name)
        return task_id

    def schedule_bulk_import(self, exchange_slug: str, symbol: str,
                             timeframes: List[str], days_back: int = 30,
                             priority: TaskPriority = TaskPriority.HIGH) -> List[str]:
        """Schedule bulk import tasks"""

        task_ids = []
        since = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)

        for timeframe in timeframes:
            task_id = self.schedule_task(
                exchange_slug=exchange_slug,
                symbol=symbol,
                timeframe=timeframe,
                priority=priority,
                since=since,
                limit=1000
            )
            task_ids.append(task_id)

        self.logger.info("Bulk import scheduled",
                         exchange=exchange_slug, symbol=symbol,
                         timeframes=len(timeframes), total_tasks=len(task_ids))

        return task_ids

    async def detect_and_fill_gaps(self, exchange: str, symbol: str, timeframe: str) -> List[str]:
        """Detect and fill data gaps"""
        task_ids = []

        try:
            async with self.session_factory() as session:
                trading_pair = await TradingPair.get_by_exchange_and_symbol(session, exchange, symbol)
                if not trading_pair:
                    return task_ids

                # Get latest timestamp
                latest_timestamp = await HistoricalData.get_latest_timestamp(
                    session, trading_pair.id, timeframe
                )

                if latest_timestamp:
                    # Check if we're missing recent data
                    time_since_last = (datetime.now() - latest_timestamp).total_seconds()
                    timeframe_seconds = self._get_timeframe_seconds(timeframe)

                    if time_since_last > timeframe_seconds * 2:
                        # Schedule task to fill gap
                        task_id = self.schedule_task(
                            exchange_slug=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            priority=TaskPriority.HIGH,
                            since=int(latest_timestamp.timestamp() * 1000),
                            limit=500
                        )
                        task_ids.append(task_id)

        except Exception as e:
            self.logger.error("Failed to detect gaps",
                              exchange=exchange, symbol=symbol,
                              timeframe=timeframe, error=str(e))

        return task_ids

    async def validate_data_integrity(self, exchange: str, symbol: str,
                                      timeframe: str, days_back: int = 7) -> Dict[str, Any]:
        """Validate data integrity"""
        try:
            async with self.session_factory() as session:
                trading_pair = await TradingPair.get_by_exchange_and_symbol(session, exchange, symbol)
                if not trading_pair:
                    return {"error": "Trading pair not found"}

                # Calculate expected vs actual records
                timeframe_minutes = self._get_timeframe_minutes(timeframe)
                expected_records = (days_back * 24 * 60) // timeframe_minutes

                # Get actual record count
                from sqlalchemy import text, func
                query = text("""
                    SELECT COUNT(*) as actual_count,
                           MIN(timestamp) as earliest,
                           MAX(timestamp) as latest
                    FROM historical_data
                    WHERE trading_pair_id = :pair_id 
                    AND timeframe = :timeframe
                    AND timestamp >= DATE_SUB(NOW(), INTERVAL :days DAY)
                """)

                result = await session.execute(query, {
                    'pair_id': trading_pair.id,
                    'timeframe': timeframe,
                    'days': days_back
                })

                row = result.fetchone()
                actual_count = row[0] if row else 0
                earliest = row[1] if row else None
                latest = row[2] if row else None

                completeness = (actual_count / expected_records) * 100 if expected_records > 0 else 0

                return {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "days_analyzed": days_back,
                    "expected_records": expected_records,
                    "actual_records": actual_count,
                    "completeness_percent": round(completeness, 2),
                    "earliest_timestamp": earliest.isoformat() if earliest else None,
                    "latest_timestamp": latest.isoformat() if latest else None,
                    "is_complete": completeness >= 95.0
                }

        except Exception as e:
            self.logger.error("Failed to validate data integrity",
                              exchange=exchange, symbol=symbol,
                              timeframe=timeframe, error=str(e))
            return {"error": str(e)}

    async def _task_processor_loop(self):
        """Process pending tasks"""
        while self.running:
            try:
                if not self.pending_tasks:
                    await asyncio.sleep(1)
                    continue

                # Get highest priority task
                task_id, task = min(self.pending_tasks.items(),
                                    key=lambda x: x[1].priority.value)

                # Remove from pending
                del self.pending_tasks[task_id]

                # Execute task
                execution_task = asyncio.create_task(
                    self._execute_task(task_id, task)
                )
                self.active_tasks[task_id] = execution_task

                # Brief pause to prevent overwhelming
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error("Task processor loop error", error=str(e))
                await asyncio.sleep(5)

    async def _execute_task(self, task_id: str, task: HistoricalTask):
        """Execute historical data task"""
        try:
            start_time = time.time()

            self.logger.info("Executing task", task_id=task_id,
                             exchange=task.exchange_slug, symbol=task.symbol,
                             timeframe=task.timeframe)

            # Get adapter
            adapter = await exchange_manager.get_public_adapter(task.exchange_slug)
            if not adapter:
                raise Exception(f"No adapter for {task.exchange_slug}")

            # Fetch data
            klines = await adapter.rest.fetch_ohlcv(
                task.symbol, task.timeframe, task.since, task.limit
            )

            if not klines:
                self.logger.warning("No data returned", task_id=task_id)
                return

            # Save to database
            saved_count = await self._save_klines_data(task, klines)

            # Record success metrics
            duration = time.time() - start_time
            self._record_completion_stats(task.exchange_slug, duration, saved_count)

            # Update monitoring
            monitoring_system.increment_counter('historical_tasks_completed',
                                                {'exchange': task.exchange_slug})
            monitoring_system.record_histogram('historical_task_duration', duration,
                                               {'exchange': task.exchange_slug})

            self.logger.info("Task completed", task_id=task_id,
                             saved_records=saved_count, duration=duration)

        except Exception as e:
            self.logger.error("Task execution failed", task_id=task_id, error=str(e))
            self._record_error_stats(task.exchange_slug)

            monitoring_system.increment_counter('historical_tasks_failed',
                                                {'exchange': task.exchange_slug})
        finally:
            # Clean up
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]

    async def _save_klines_data(self, task: HistoricalTask, klines: List[List]) -> int:
        """Save klines data to database"""
        if not klines:
            return 0

        async with self.session_factory() as session:
            try:
                trading_pair = await TradingPair.get_by_exchange_and_symbol(
                    session, task.exchange_slug, task.symbol
                )
                if not trading_pair:
                    return 0

                return await HistoricalData.save_klines(
                    session, trading_pair.id, task.timeframe, klines
                )

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to save klines data", error=str(e))
                return 0

    def _record_completion_stats(self, exchange: str, duration: float, records_saved: int):
        """Record completion statistics"""
        if exchange not in self.completion_stats:
            self.completion_stats[exchange] = []

        self.completion_stats[exchange].append(duration)

        # Keep only recent stats
        if len(self.completion_stats[exchange]) > 100:
            self.completion_stats[exchange] = self.completion_stats[exchange][-50:]

    def _record_error_stats(self, exchange: str):
        """Record error statistics"""
        current_rate = self.error_rates.get(exchange, 0)
        self.error_rates[exchange] = min(current_rate + 0.1, 1.0)

    def _get_timeframe_minutes(self, timeframe: str) -> int:
        """Convert timeframe to minutes"""
        timeframe_map = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4h': 240, '1d': 1440
        }
        return timeframe_map.get(timeframe, 60)

    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """Convert timeframe to seconds"""
        return self._get_timeframe_minutes(timeframe) * 60

    async def get_optimization_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            'pending_tasks': len(self.pending_tasks),
            'active_tasks': len(self.active_tasks),
            'completion_stats': {
                exchange: {
                    'avg_duration': sum(times) / len(times) if times else 0,
                    'total_completions': len(times)
                } for exchange, times in self.completion_stats.items()
            },
            'error_rates': dict(self.error_rates),
            'running': self.running
        }

    def get_service_stats(self) -> Dict[str, Any]:
        """Get service statistics (alias)"""
        return {
            'running': self.running,
            'pending_tasks': len(self.pending_tasks),
            'active_tasks': len(self.active_tasks)
        }

    def schedule_incremental_update(self, exchange_slug: str, symbol: str, timeframe: str):
        """Schedule incremental update"""
        return self.schedule_task(
            exchange_slug=exchange_slug,
            symbol=symbol,
            timeframe=timeframe,
            priority=TaskPriority.HIGH,
            limit=100
        )

    async def schedule_optimized_task(self, exchange_slug: str, symbol: str, timeframe: str,
                                      priority: TaskPriority = TaskPriority.MEDIUM,
                                      since: Optional[int] = None, limit: int = 1000) -> str:
        """Schedule optimized task (alias for compatibility)"""
        return self.schedule_task(exchange_slug, symbol, timeframe, priority, since, limit)


# Global instance
historical_data_service = HistoricalDataService()

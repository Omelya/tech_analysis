import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import structlog

from app.utils.rate_limiter import rate_limiter, RequestType
from .request_scheduler import request_scheduler
from ..exchanges.manager import exchange_manager
from ..models.database import TradingPair, HistoricalData
from ..utils.metrics_collector import metrics_collector


class TaskPriority(Enum):
    """Optimized task priority levels"""
    CRITICAL = 0  # Real-time gaps, immediate needs
    HIGH = 1  # Recent data gaps (< 24h)
    MEDIUM = 2  # Regular updates (< 7 days)
    LOW = 3  # Bulk historical (> 7 days)
    BACKGROUND = 4  # Archive/cleanup tasks


@dataclass
class HistoricalTask:
    """Optimized historical data task with smart scheduling"""
    exchange_slug: str
    symbol: str
    timeframe: str
    priority: TaskPriority
    since: Optional[int] = None
    limit: int = 1000
    chunk_size: int = 500
    created_at: float = None
    estimated_requests: int = 1

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


class OptimizedHistoricalDataService:
    """Optimized historical data service with intelligent request management"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="optimized_historical_service")

        # Task management
        self.pending_tasks: Dict[str, HistoricalTask] = {}
        self.active_tasks: Dict[str, asyncio.Task] = {}

        # Optimization strategies
        self.batch_strategies = {
            'binance': {'max_batch_size': 1000, 'preferred_chunk': 500},
            'bybit': {'max_batch_size': 500, 'preferred_chunk': 200},
            'whitebit': {'max_batch_size': 200, 'preferred_chunk': 100}
        }

        # Smart scheduling
        self.running = False
        self.scheduler_task: Optional[asyncio.Task] = None

        # Performance tracking
        self.completion_stats: Dict[str, List[float]] = {}
        self.error_rates: Dict[str, float] = {}

    async def initialize(self, session_factory):
        """Initialize optimized service"""
        self.session_factory = session_factory
        self.running = True

        # Initialize smart scheduler
        await request_scheduler.initialize()

        # Start task scheduler
        self.scheduler_task = asyncio.create_task(self._optimized_scheduler_loop())

        self.logger.info("Optimized historical data service initialized")

    async def schedule_optimized_task(self, exchange_slug: str, symbol: str, timeframe: str,
                                      priority: TaskPriority = TaskPriority.MEDIUM,
                                      since: Optional[int] = None, limit: int = 1000) -> str:
        """Schedule optimized historical data task"""

        # Create optimized task
        task = HistoricalTask(
            exchange_slug=exchange_slug,
            symbol=symbol,
            timeframe=timeframe,
            priority=priority,
            since=since,
            limit=limit
        )

        # Optimize task parameters
        await self._optimize_task_parameters(task)

        # Generate task ID
        task_id = f"{exchange_slug}_{symbol}_{timeframe}_{int(task.created_at)}"
        self.pending_tasks[task_id] = task

        self.logger.info("Optimized task scheduled",
                         task_id=task_id, priority=priority.name,
                         estimated_requests=task.estimated_requests)

        return task_id

    async def _optimize_task_parameters(self, task: HistoricalTask):
        """Optimize task parameters based on exchange capacity and data requirements"""

        # Get current exchange capacity
        capacity = rate_limiter.get_exchange_capacity(task.exchange_slug)

        # Get exchange-specific strategy
        strategy = self.batch_strategies.get(task.exchange_slug, {
            'max_batch_size': 500, 'preferred_chunk': 200
        })

        # Calculate optimal chunk size
        remaining_capacity = capacity.get('requests_remaining', 100)

        if remaining_capacity > 500:
            task.chunk_size = strategy['max_batch_size']
        elif remaining_capacity > 100:
            task.chunk_size = strategy['preferred_chunk']
        else:
            task.chunk_size = min(strategy['preferred_chunk'] // 2, 100)

        # Estimate number of requests needed
        if task.since:
            timeframe_minutes = self._get_timeframe_minutes(task.timeframe)
            time_range = (int(time.time() * 1000) - task.since) // (1000 * 60)
            candles_needed = time_range // timeframe_minutes
            task.estimated_requests = max(1, (candles_needed + task.chunk_size - 1) // task.chunk_size)
        else:
            task.estimated_requests = max(1, (task.limit + task.chunk_size - 1) // task.chunk_size)

    async def _optimized_scheduler_loop(self):
        """Optimized scheduler loop with intelligent task distribution"""
        while self.running:
            try:
                if not self.pending_tasks:
                    await asyncio.sleep(5)
                    continue

                # Group tasks by exchange for batch optimization
                exchange_tasks = self._group_tasks_by_exchange()

                # Process each exchange's tasks optimally
                for exchange, tasks in exchange_tasks.items():
                    await self._process_exchange_tasks_optimally(exchange, tasks)

                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error("Error in optimized scheduler loop", error=str(e))
                await asyncio.sleep(10)

    def _group_tasks_by_exchange(self) -> Dict[str, List[Tuple[str, HistoricalTask]]]:
        """Group pending tasks by exchange for batch optimization"""
        exchange_tasks = {}

        for task_id, task in self.pending_tasks.items():
            exchange = task.exchange_slug
            if exchange not in exchange_tasks:
                exchange_tasks[exchange] = []
            exchange_tasks[exchange].append((task_id, task))

        return exchange_tasks

    async def _process_exchange_tasks_optimally(self, exchange: str,
                                                tasks: List[Tuple[str, HistoricalTask]]):
        """Process exchange tasks with optimal resource allocation"""

        capacity = rate_limiter.get_exchange_capacity(exchange)
        remaining_requests = capacity.get('requests_remaining', 0)

        if remaining_requests < 10:
            return  # Wait for capacity

        # Sort tasks by priority and estimated completion time
        sorted_tasks = sorted(tasks, key=lambda x: (
            x[1].priority.value,
            x[1].estimated_requests,
            x[1].created_at
        ))

        # Process tasks within capacity limits
        processed_count = 0
        request_budget = min(remaining_requests // 2, 50)  # Conservative approach

        for task_id, task in sorted_tasks:
            if processed_count >= request_budget:
                break

            if task.estimated_requests <= (request_budget - processed_count):
                # Start task execution
                await self._execute_optimized_task(task_id, task)
                processed_count += task.estimated_requests

                # Remove from pending
                del self.pending_tasks[task_id]

    async def _execute_optimized_task(self, task_id: str, task: HistoricalTask):
        """Execute optimized historical data task"""

        self.logger.info("Executing optimized task", task_id=task_id,
                         exchange=task.exchange_slug, symbol=task.symbol,
                         timeframe=task.timeframe)

        # Create execution task
        execution_task = asyncio.create_task(
            self._smart_historical_fetch(task_id, task)
        )

        self.active_tasks[task_id] = execution_task

    async def _smart_historical_fetch(self, task_id: str, task: HistoricalTask):
        """Smart historical data fetching with chunked requests"""
        try:
            start_time = time.time()
            total_saved = 0

            # Calculate chunks needed
            chunks = self._calculate_optimal_chunks(task)

            self.logger.info("Starting smart fetch", task_id=task_id,
                             chunks=len(chunks), chunk_size=task.chunk_size)

            # Process chunks with smart scheduling
            for i, (chunk_since, chunk_limit) in enumerate(chunks):

                # Define the request function
                async def fetch_chunk():
                    adapter = await exchange_manager.get_public_adapter(task.exchange_slug)
                    if not adapter:
                        raise Exception(f"No adapter for {task.exchange_slug}")

                    return await adapter.rest.fetch_ohlcv(
                        task.symbol, task.timeframe, chunk_since, chunk_limit
                    )

                # Define success callback
                async def chunk_callback(result, error):
                    nonlocal total_saved
                    if error:
                        self.logger.error("Chunk fetch failed",
                                          task_id=task_id, chunk=i, error=str(error))
                    elif result:
                        saved = await self._save_chunk_data(task, result)
                        total_saved += saved
                        self.logger.debug("Chunk processed",
                                          task_id=task_id, chunk=i, saved=saved)

                # Schedule chunk request with smart scheduler
                await request_scheduler.schedule_request(
                    exchange=task.exchange_slug,
                    request_type=RequestType.HISTORICAL_BULK,
                    function=fetch_chunk,
                    priority=task.priority.value * 10,
                    callback=chunk_callback
                )

                # Brief pause between chunk scheduling
                await asyncio.sleep(0.1)

            # Record completion metrics
            completion_time = time.time() - start_time
            self._record_completion_stats(task.exchange_slug, completion_time, total_saved)

            self.logger.info("Smart fetch completed", task_id=task_id,
                             total_saved=total_saved, duration=completion_time)

        except Exception as e:
            self.logger.error("Smart fetch failed", task_id=task_id, error=str(e))
            self._record_error_stats(task.exchange_slug)
        finally:
            # Clean up
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]

    def _calculate_optimal_chunks(self, task: HistoricalTask) -> List[Tuple[Optional[int], int]]:
        """Calculate optimal chunks for data fetching"""
        chunks = []

        if not task.since:
            # Simple case: just one chunk with limit
            chunks.append((None, min(task.limit, task.chunk_size)))
        else:
            # Calculate time-based chunks
            current_time = int(time.time() * 1000)
            timeframe_ms = self._get_timeframe_minutes(task.timeframe) * 60 * 1000

            chunk_time_span = task.chunk_size * timeframe_ms
            current_since = task.since

            while current_since < current_time:
                chunk_until = min(current_since + chunk_time_span, current_time)
                chunks.append((current_since, task.chunk_size))
                current_since = chunk_until

                if len(chunks) >= task.estimated_requests:
                    break

        return chunks

    async def _save_chunk_data(self, task: HistoricalTask, klines: List[List]) -> int:
        """Save chunk data to database"""
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
                self.logger.error("Failed to save chunk data", error=str(e))
                return 0

    def _record_completion_stats(self, exchange: str, duration: float, records_saved: int):
        """Record completion statistics"""
        if exchange not in self.completion_stats:
            self.completion_stats[exchange] = []

        self.completion_stats[exchange].append(duration)

        # Keep only recent stats
        if len(self.completion_stats[exchange]) > 100:
            self.completion_stats[exchange] = self.completion_stats[exchange][-50:]

        # Record metrics
        metrics_collector.record_histogram('historical_task_duration', duration,
                                           {'exchange': exchange})
        metrics_collector.record_data_processing('historical', exchange, '',
                                                 records_saved, duration)

    def _record_error_stats(self, exchange: str):
        """Record error statistics"""
        current_rate = self.error_rates.get(exchange, 0)
        self.error_rates[exchange] = min(current_rate + 0.1, 1.0)

        metrics_collector.increment_counter('historical_task_errors', {'exchange': exchange})

    def _get_timeframe_minutes(self, timeframe: str) -> int:
        """Convert timeframe to minutes"""
        timeframe_map = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4h': 240, '1d': 1440
        }
        return timeframe_map.get(timeframe, 60)

    async def bulk_import_with_optimization(self, exchange_slug: str, symbols: List[str],
                                            timeframes: List[str], days_back: int = 30) -> List[str]:
        """Bulk import with full optimization"""

        # Calculate optimal distribution strategy
        strategy = rate_limiter.calculate_bulk_fetch_strategy(
            exchange_slug, symbols[0], timeframes[0], days_back
        )

        if not strategy['can_start_immediately']:
            self.logger.warning("Bulk import delayed due to capacity constraints",
                                exchange=exchange_slug,
                                estimated_wait=strategy['estimated_time_seconds'])

        task_ids = []

        # Optimize timeframe order
        optimal_timeframes = rate_limiter.suggest_optimal_timeframes(
            exchange_slug, timeframes
        )

        # Schedule tasks with staggered priorities
        base_priority = TaskPriority.MEDIUM

        for i, symbol in enumerate(symbols):
            for j, timeframe in enumerate(optimal_timeframes):
                # Stagger priorities to distribute load
                priority_offset = (i + j) % 3
                if priority_offset == 0:
                    priority = TaskPriority.HIGH
                elif priority_offset == 1:
                    priority = base_priority
                else:
                    priority = TaskPriority.LOW

                task_id = await self.schedule_optimized_task(
                    exchange_slug=exchange_slug,
                    symbol=symbol,
                    timeframe=timeframe,
                    priority=priority,
                    since=int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000),
                    limit=strategy['batch_size']
                )

                task_ids.append(task_id)

        self.logger.info("Bulk import scheduled with optimization",
                         exchange=exchange_slug, symbols=len(symbols),
                         timeframes=len(optimal_timeframes), total_tasks=len(task_ids))

        return task_ids

    async def get_optimization_stats(self) -> Dict[str, Any]:
        """Get optimization statistics"""
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
            'rate_limit_stats': rate_limiter.get_optimizer_stats(),
            'scheduler_stats': await request_scheduler.get_queue_stats() if request_scheduler else {}
        }

    async def shutdown(self):
        """Shutdown optimized service"""
        self.running = False

        if self.scheduler_task:
            self.scheduler_task.cancel()

        # Cancel active tasks
        for task in self.active_tasks.values():
            task.cancel()

        await request_scheduler.shutdown()

        self.logger.info("Optimized historical data service shutdown complete")


# Global instance
historical_data_service = OptimizedHistoricalDataService()

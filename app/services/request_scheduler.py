import asyncio
import heapq
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import structlog

from app.utils.rate_limiter import rate_limiter, RequestType


@dataclass
class SmartRequest:
    """Smart request with scheduling information"""
    priority: int
    exchange: str
    request_type: RequestType
    function: Callable
    args: tuple
    kwargs: dict
    created_at: float = field(default_factory=time.time)
    retries: int = 0
    max_retries: int = 3
    callback: Optional[Callable] = None

    def __lt__(self, other):
        return self.priority < other.priority


class TimeWindow(Enum):
    """Time windows for request distribution"""
    IMMEDIATE = 0  # 0-5 seconds
    SHORT = 1  # 5-60 seconds
    MEDIUM = 2  # 1-5 minutes
    LONG = 3  # 5+ minutes


class SmartRequestScheduler:
    """Intelligent request scheduler that optimizes API usage"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="smart_scheduler")

        # Request queues by exchange and time window
        self.request_queues: Dict[str, Dict[TimeWindow, List[SmartRequest]]] = {}

        # Active workers per exchange
        self.workers: Dict[str, asyncio.Task] = {}
        self.worker_counts: Dict[str, int] = {}

        # Request distribution strategies
        self.distribution_strategies = {
            'binance': self._binance_distribution_strategy,
            'bybit': self._bybit_distribution_strategy,
            'whitebit': self._whitebit_distribution_strategy
        }

        # Scheduling state
        self.running = False
        self.pause_until: Dict[str, float] = {}

    async def initialize(self):
        """Initialize smart scheduler"""
        self.running = True

        # Initialize queues for supported exchanges
        for exchange in ['binance', 'bybit', 'whitebit']:
            self.request_queues[exchange] = {
                TimeWindow.IMMEDIATE: [],
                TimeWindow.SHORT: [],
                TimeWindow.MEDIUM: [],
                TimeWindow.LONG: []
            }

            # Start worker for each exchange
            self.workers[exchange] = asyncio.create_task(self._exchange_worker(exchange))
            self.worker_counts[exchange] = 1

        self.logger.info("Smart request scheduler initialized")

    async def schedule_request(self, exchange: str, request_type: RequestType,
                               function: Callable, *args, priority: int = 100,
                               callback: Optional[Callable] = None, **kwargs) -> str:
        """Schedule a smart request"""
        if exchange not in self.request_queues:
            raise ValueError(f"Unsupported exchange: {exchange}")

        # Create smart request
        request = SmartRequest(
            priority=priority,
            exchange=exchange,
            request_type=request_type,
            function=function,
            args=args,
            kwargs=kwargs,
            callback=callback
        )

        # Determine optimal time window
        time_window = self._determine_time_window(exchange, request_type)

        # Add to appropriate queue
        queue = self.request_queues[exchange][time_window]
        heapq.heappush(queue, request)

        request_id = f"{exchange}_{request_type.name}_{int(time.time() * 1000)}"

        self.logger.debug("Request scheduled",
                          exchange=exchange, request_type=request_type.name,
                          time_window=time_window.name, queue_size=len(queue))

        return request_id

    def _determine_time_window(self, exchange: str, request_type: RequestType) -> TimeWindow:
        """Determine optimal time window for request"""
        capacity = rate_limiter.get_exchange_capacity(exchange)

        requests_remaining = capacity.get('requests_remaining', 0)
        consecutive_limits = capacity.get('consecutive_rate_limits', 0)

        # If exchange is under pressure, distribute to longer windows
        if consecutive_limits > 3:
            return TimeWindow.LONG
        elif consecutive_limits > 1:
            return TimeWindow.MEDIUM
        elif requests_remaining < 50:
            return TimeWindow.SHORT
        else:
            return TimeWindow.IMMEDIATE

    async def _exchange_worker(self, exchange: str):
        """Worker that processes requests for an exchange"""
        while self.running:
            try:
                # Check if exchange is paused
                if exchange in self.pause_until and time.time() < self.pause_until[exchange]:
                    await asyncio.sleep(1)
                    continue

                # Process requests from different time windows
                await self._process_time_windows(exchange)

                # Brief pause between processing cycles
                await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error("Exchange worker error", exchange=exchange, error=str(e))
                await asyncio.sleep(5)

    async def _process_time_windows(self, exchange: str):
        """Process requests from different time windows"""
        queues = self.request_queues[exchange]

        # Process immediate requests first
        if queues[TimeWindow.IMMEDIATE]:
            await self._process_window_requests(exchange, TimeWindow.IMMEDIATE, max_requests=5)

        # Then short-term requests
        elif queues[TimeWindow.SHORT]:
            await self._process_window_requests(exchange, TimeWindow.SHORT, max_requests=3)

        # Then medium-term requests
        elif queues[TimeWindow.MEDIUM]:
            await self._process_window_requests(exchange, TimeWindow.MEDIUM, max_requests=2)

        # Finally long-term requests
        elif queues[TimeWindow.LONG]:
            await self._process_window_requests(exchange, TimeWindow.LONG, max_requests=1)

    async def _process_window_requests(self, exchange: str, window: TimeWindow, max_requests: int):
        """Process requests from specific time window"""
        queue = self.request_queues[exchange][window]
        processed = 0

        while queue and processed < max_requests:
            # Get highest priority request
            request = heapq.heappop(queue)

            # Check if request should be delayed
            if self._should_delay_request(exchange, request, window):
                # Move to next time window
                next_window = self._get_next_window(window)
                if next_window:
                    heapq.heappush(self.request_queues[exchange][next_window], request)
                else:
                    # Re-queue in same window
                    heapq.heappush(queue, request)
                break

            # Execute request
            await self._execute_smart_request(request)
            processed += 1

    def _should_delay_request(self, exchange: str, request: SmartRequest, window: TimeWindow) -> bool:
        """Determine if request should be delayed"""
        capacity = rate_limiter.get_exchange_capacity(exchange)

        # Check capacity thresholds
        requests_remaining = capacity.get('requests_remaining', 0)
        weight_remaining = capacity.get('weight_remaining', 0)

        # Apply window-specific thresholds
        if window == TimeWindow.IMMEDIATE:
            return requests_remaining < 10 or weight_remaining < request.request_type.value * 5
        elif window == TimeWindow.SHORT:
            return requests_remaining < 5 or weight_remaining < request.request_type.value * 3
        elif window == TimeWindow.MEDIUM:
            return requests_remaining < 2 or weight_remaining < request.request_type.value * 2
        else:  # LONG
            return requests_remaining < 1 or weight_remaining < request.request_type.value

    def _get_next_window(self, current: TimeWindow) -> Optional[TimeWindow]:
        """Get next time window for delaying request"""
        window_order = [TimeWindow.IMMEDIATE, TimeWindow.SHORT, TimeWindow.MEDIUM, TimeWindow.LONG]

        try:
            current_idx = window_order.index(current)
            if current_idx < len(window_order) - 1:
                return window_order[current_idx + 1]
        except ValueError:
            pass

        return None

    async def _execute_smart_request(self, request: SmartRequest):
        """Execute a smart request with full optimization"""
        try:
            # Use rate limit optimizer
            result = await rate_limiter.schedule_request(
                request.exchange,
                request.request_type,
                request.function,
                *request.args,
                **request.kwargs
            )

            # Call success callback
            if request.callback:
                await self._safe_callback(request.callback, result, None)

            self.logger.debug("Smart request executed successfully",
                              exchange=request.exchange, request_type=request.request_type.name)

        except Exception as e:
            # Handle failure
            await self._handle_request_failure(request, e)

    async def _handle_request_failure(self, request: SmartRequest, error: Exception):
        """Handle request failure with retry logic"""
        self.logger.warning("Smart request failed",
                            exchange=request.exchange,
                            request_type=request.request_type.name,
                            error=str(error), retries=request.retries)

        # Check if it's a rate limit error
        if 'rate limit' in str(error).lower() or '429' in str(error):
            # Pause exchange temporarily
            pause_duration = min(60 * (2 ** request.retries), 300)  # Max 5 minutes
            self.pause_until[request.exchange] = time.time() + pause_duration

            self.logger.info("Pausing exchange due to rate limit",
                             exchange=request.exchange, pause_seconds=pause_duration)

        # Retry if possible
        if request.retries < request.max_retries:
            request.retries += 1

            # Move to longer time window for retry
            time_window = TimeWindow.LONG if request.retries > 1 else TimeWindow.MEDIUM
            heapq.heappush(self.request_queues[request.exchange][time_window], request)

        else:
            # Call failure callback
            if request.callback:
                await self._safe_callback(request.callback, None, error)

    async def _safe_callback(self, callback: Callable, result: Any, error: Optional[Exception]):
        """Safely execute callback"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(result, error)
            else:
                callback(result, error)
        except Exception as e:
            self.logger.error("Callback execution failed", error=str(e))

    # Exchange-specific distribution strategies
    def _binance_distribution_strategy(self, requests: List[SmartRequest]) -> Dict[TimeWindow, List[SmartRequest]]:
        """Binance-specific request distribution"""
        distribution = {window: [] for window in TimeWindow}

        for request in requests:
            if request.request_type == RequestType.TICKER:
                distribution[TimeWindow.IMMEDIATE].append(request)
            elif request.request_type == RequestType.OHLCV:
                distribution[TimeWindow.SHORT].append(request)
            elif request.request_type == RequestType.HISTORICAL_BULK:
                distribution[TimeWindow.MEDIUM].append(request)
            else:
                distribution[TimeWindow.SHORT].append(request)

        return distribution

    def _bybit_distribution_strategy(self, requests: List[SmartRequest]) -> Dict[TimeWindow, List[SmartRequest]]:
        """Bybit-specific request distribution"""
        distribution = {window: [] for window in TimeWindow}

        # Bybit has lower limits, distribute more conservatively
        for request in requests:
            if request.request_type == RequestType.TICKER:
                distribution[TimeWindow.SHORT].append(request)
            elif request.request_type == RequestType.OHLCV:
                distribution[TimeWindow.MEDIUM].append(request)
            else:
                distribution[TimeWindow.LONG].append(request)

        return distribution

    def _whitebit_distribution_strategy(self, requests: List[SmartRequest]) -> Dict[TimeWindow, List[SmartRequest]]:
        """WhiteBit-specific request distribution"""
        distribution = {window: [] for window in TimeWindow}

        # WhiteBit has the lowest limits, most conservative distribution
        for request in requests:
            if request.request_type == RequestType.TICKER:
                distribution[TimeWindow.MEDIUM].append(request)
            else:
                distribution[TimeWindow.LONG].append(request)

        return distribution

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        stats = {}

        for exchange, queues in self.request_queues.items():
            exchange_stats = {}
            total_requests = 0

            for window, queue in queues.items():
                queue_size = len(queue)
                exchange_stats[window.name.lower()] = queue_size
                total_requests += queue_size

            exchange_stats['total'] = total_requests
            exchange_stats['paused_until'] = self.pause_until.get(exchange, 0)
            exchange_stats['worker_active'] = not self.workers[exchange].done()

            stats[exchange] = exchange_stats

        return stats

    async def optimize_queue_distribution(self, exchange: str):
        """Optimize queue distribution based on current conditions"""
        if exchange not in self.distribution_strategies:
            return

        strategy = self.distribution_strategies[exchange]
        queues = self.request_queues[exchange]

        # Collect all requests from all queues
        all_requests = []
        for queue in queues.values():
            all_requests.extend(queue)
            queue.clear()

        # Redistribute using exchange strategy
        new_distribution = strategy(all_requests)

        # Update queues
        for window, requests in new_distribution.items():
            for request in requests:
                heapq.heappush(queues[window], request)

    async def shutdown(self):
        """Shutdown smart scheduler"""
        self.running = False

        # Cancel all workers
        for worker in self.workers.values():
            worker.cancel()

        # Wait for workers to complete
        await asyncio.gather(*self.workers.values(), return_exceptions=True)

        self.logger.info("Smart request scheduler shutdown complete")


# Global instance
request_scheduler = SmartRequestScheduler()

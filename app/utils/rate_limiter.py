import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import structlog
from datetime import datetime, timedelta


class RequestType(Enum):
    """Types of API requests with different weights"""
    TICKER = 1
    OHLCV = 2
    ORDERBOOK = 5
    MARKETS = 10
    HISTORICAL_BULK = 50


@dataclass
class RateWindow:
    """Rate limiting window"""
    requests: int = 0
    weight: int = 0
    reset_time: float = 0
    window_size: int = 60  # seconds


class RateLimitOptimizer:
    """Optimizes API requests to avoid rate limits"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="rate_limit_optimizer")

        # Exchange-specific rate limits (requests per minute)
        self.exchange_limits = {
            'binance': {
                'requests_per_minute': 1200,
                'weight_per_minute': 6000,
                'burst_limit': 20,
                'cooldown_multiplier': 1.5
            },
            'bybit': {
                'requests_per_minute': 600,
                'weight_per_minute': 3000,
                'burst_limit': 10,
                'cooldown_multiplier': 2.0
            },
            'whitebit': {
                'requests_per_minute': 300,
                'weight_per_minute': 1500,
                'burst_limit': 5,
                'cooldown_multiplier': 3.0
            }
        }

        # Current usage tracking
        self.current_usage: Dict[str, RateWindow] = {}
        self.request_queue: Dict[str, List] = {}
        self.last_request_time: Dict[str, float] = {}

        # Adaptive delays
        self.adaptive_delays: Dict[str, float] = {}
        self.consecutive_limits: Dict[str, int] = {}

        # Request scheduling
        self.scheduled_requests: Dict[str, asyncio.Task] = {}

    def get_optimal_delay(self, exchange: str, request_type: RequestType) -> float:
        """Calculate optimal delay between requests"""
        if exchange not in self.exchange_limits:
            return 1.0

        limits = self.exchange_limits[exchange]
        current = self._get_current_window(exchange)

        # Base delay calculation
        requests_per_second = limits['requests_per_minute'] / 60
        base_delay = 1.0 / requests_per_second

        # Apply request weight
        weight_factor = request_type.value
        weighted_delay = base_delay * weight_factor

        # Adaptive adjustment based on recent rate limit hits
        adaptive_factor = self.adaptive_delays.get(exchange, 1.0)
        final_delay = weighted_delay * adaptive_factor

        # Consider current usage
        usage_factor = self._calculate_usage_factor(exchange, current)
        final_delay *= usage_factor

        return max(final_delay, 0.1)  # Minimum 100ms

    def _get_current_window(self, exchange: str) -> RateWindow:
        """Get current rate limiting window"""
        current_time = time.time()

        if exchange not in self.current_usage:
            self.current_usage[exchange] = RateWindow(reset_time=current_time + 60)

        window = self.current_usage[exchange]

        # Reset window if expired
        if current_time >= window.reset_time:
            window.requests = 0
            window.weight = 0
            window.reset_time = current_time + 60

        return window

    def _calculate_usage_factor(self, exchange: str, window: RateWindow) -> float:
        """Calculate usage-based delay factor"""
        if exchange not in self.exchange_limits:
            return 1.0

        limits = self.exchange_limits[exchange]

        # Request usage percentage
        request_usage = window.requests / limits['requests_per_minute']

        # Weight usage percentage
        weight_usage = window.weight / limits['weight_per_minute']

        # Use higher usage for calculation
        max_usage = max(request_usage, weight_usage)

        # Progressive delay increase
        if max_usage > 0.9:
            return 10.0  # Severe throttling
        elif max_usage > 0.8:
            return 5.0  # Heavy throttling
        elif max_usage > 0.6:
            return 2.0  # Moderate throttling
        elif max_usage > 0.4:
            return 1.5  # Light throttling
        else:
            return 1.0  # No additional throttling

    async def schedule_request(self, exchange: str, request_type: RequestType,
                               request_func, *args, **kwargs) -> Any:
        """Schedule request with optimal timing"""
        try:
            # Calculate optimal delay
            delay = self.get_optimal_delay(exchange, request_type)

            # Check if we need to wait since last request
            if exchange in self.last_request_time:
                time_since_last = time.time() - self.last_request_time[exchange]
                if time_since_last < delay:
                    wait_time = delay - time_since_last
                    await asyncio.sleep(wait_time)

            # Update tracking before request
            self._update_usage_before_request(exchange, request_type)

            # Execute request
            start_time = time.time()
            result = await request_func(*args, **kwargs)
            request_time = time.time() - start_time

            # Update tracking after successful request
            self._update_usage_after_request(exchange, request_type, True, request_time)

            return result

        except Exception as e:
            # Handle rate limit errors
            if 'rate limit' in str(e).lower() or '429' in str(e):
                await self._handle_rate_limit_error(exchange, request_type)

            # Update tracking after failed request
            self._update_usage_after_request(exchange, request_type, False, 0)
            raise

    def _update_usage_before_request(self, exchange: str, request_type: RequestType):
        """Update usage tracking before making request"""
        window = self._get_current_window(exchange)
        window.requests += 1
        window.weight += request_type.value
        self.last_request_time[exchange] = time.time()

    def _update_usage_after_request(self, exchange: str, request_type: RequestType,
                                    success: bool, request_time: float):
        """Update usage tracking after request completion"""
        if success:
            # Reset consecutive limit hits on success
            self.consecutive_limits[exchange] = 0

            # Gradually reduce adaptive delay
            if exchange in self.adaptive_delays:
                self.adaptive_delays[exchange] = max(
                    1.0, self.adaptive_delays[exchange] * 0.95
                )
        else:
            # Increase consecutive limit hits
            self.consecutive_limits[exchange] = self.consecutive_limits.get(exchange, 0) + 1

    async def _handle_rate_limit_error(self, exchange: str, request_type: RequestType):
        """Handle rate limit error with adaptive backoff"""
        self.logger.warning("Rate limit hit", exchange=exchange, request_type=request_type.name)

        # Increase adaptive delay
        limits = self.exchange_limits.get(exchange, {})
        multiplier = limits.get('cooldown_multiplier', 2.0)

        current_delay = self.adaptive_delays.get(exchange, 1.0)
        new_delay = min(current_delay * multiplier, 60.0)  # Max 60 seconds
        self.adaptive_delays[exchange] = new_delay

        # Increase consecutive hits
        consecutive = self.consecutive_limits.get(exchange, 0) + 1
        self.consecutive_limits[exchange] = consecutive

        # Progressive backoff based on consecutive hits
        backoff_time = min(30 * consecutive, 300)  # Max 5 minutes

        self.logger.info("Applying rate limit backoff",
                         exchange=exchange, backoff_seconds=backoff_time,
                         consecutive_hits=consecutive)

        await asyncio.sleep(backoff_time)

    def batch_requests(self, exchange: str, requests: List[Tuple[RequestType, callable, tuple, dict]]) -> List:
        """Batch multiple requests with optimal spacing"""
        if not requests:
            return []

        # Sort by priority (lower RequestType value = higher priority)
        sorted_requests = sorted(requests, key=lambda x: x[0].value)

        return sorted_requests

    async def execute_batched_requests(self, exchange: str,
                                       requests: List[Tuple[RequestType, callable, tuple, dict]]) -> List[Any]:
        """Execute batched requests with optimal timing"""
        results = []

        for request_type, func, args, kwargs in requests:
            try:
                result = await self.schedule_request(exchange, request_type, func, *args, **kwargs)
                results.append(result)
            except Exception as e:
                self.logger.error("Batched request failed",
                                  exchange=exchange, request_type=request_type.name, error=str(e))
                results.append(None)

        return results

    def get_exchange_capacity(self, exchange: str) -> Dict[str, Any]:
        """Get current capacity information for exchange"""
        if exchange not in self.exchange_limits:
            return {}

        limits = self.exchange_limits[exchange]
        window = self._get_current_window(exchange)

        time_until_reset = max(0, window.reset_time - time.time())

        return {
            'requests_remaining': max(0, limits['requests_per_minute'] - window.requests),
            'weight_remaining': max(0, limits['weight_per_minute'] - window.weight),
            'requests_used': window.requests,
            'weight_used': window.weight,
            'time_until_reset': time_until_reset,
            'adaptive_delay_factor': self.adaptive_delays.get(exchange, 1.0),
            'consecutive_rate_limits': self.consecutive_limits.get(exchange, 0),
            'recommended_delay': self.get_optimal_delay(exchange, RequestType.OHLCV)
        }

    def suggest_optimal_timeframes(self, exchange: str, available_timeframes: List[str]) -> List[str]:
        """Suggest optimal timeframes based on current capacity"""
        capacity = self.get_exchange_capacity(exchange)

        # If capacity is low, prioritize larger timeframes
        if capacity.get('requests_remaining', 0) < 100:
            priority_order = ['1d', '4h', '1h', '30m', '15m', '5m', '1m']
        else:
            priority_order = ['1h', '4h', '1d', '30m', '15m', '5m', '1m']

        # Return timeframes in optimal order
        result = []
        for tf in priority_order:
            if tf in available_timeframes:
                result.append(tf)

        return result

    def calculate_bulk_fetch_strategy(self, exchange: str, symbol: str,
                                      timeframe: str, days_back: int) -> Dict[str, Any]:
        """Calculate optimal strategy for bulk historical data fetch"""
        capacity = self.get_exchange_capacity(exchange)

        # Calculate required requests
        timeframe_minutes = self._get_timeframe_minutes(timeframe)
        total_candles = (days_back * 24 * 60) // timeframe_minutes

        # Determine batch size based on exchange and capacity
        if exchange == 'binance':
            max_batch_size = 1000
        elif exchange == 'bybit':
            max_batch_size = 500
        else:  # whitebit
            max_batch_size = 200

        # Adjust batch size based on current capacity
        remaining_requests = capacity.get('requests_remaining', 100)
        if remaining_requests < 100:
            batch_size = min(max_batch_size, 100)
        else:
            batch_size = max_batch_size

        batches_needed = (total_candles + batch_size - 1) // batch_size
        estimated_time = batches_needed * self.get_optimal_delay(exchange, RequestType.HISTORICAL_BULK)

        return {
            'total_candles': total_candles,
            'batch_size': batch_size,
            'batches_needed': batches_needed,
            'estimated_time_seconds': estimated_time,
            'recommended_delay': self.get_optimal_delay(exchange, RequestType.HISTORICAL_BULK),
            'can_start_immediately': remaining_requests > batches_needed
        }

    def _get_timeframe_minutes(self, timeframe: str) -> int:
        """Convert timeframe to minutes"""
        timeframe_map = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4h': 240, '1d': 1440
        }
        return timeframe_map.get(timeframe, 60)

    def get_optimizer_stats(self) -> Dict[str, Any]:
        """Get optimizer statistics"""
        stats = {}

        for exchange in self.exchange_limits.keys():
            capacity = self.get_exchange_capacity(exchange)
            stats[exchange] = {
                **capacity,
                'last_request_time': self.last_request_time.get(exchange, 0),
                'scheduled_requests': len(self.scheduled_requests.get(exchange, []))
            }

        return {
            'exchanges': stats,
            'total_adaptive_delays': len(self.adaptive_delays),
            'total_rate_limit_hits': sum(self.consecutive_limits.values())
        }


# Global instance
rate_limiter = RateLimitOptimizer()

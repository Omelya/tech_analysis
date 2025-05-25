import asyncio
import time
from typing import Dict, List, Optional, Any, Callable, Set
from dataclasses import dataclass
from enum import Enum
import structlog
import psutil
from datetime import datetime, timedelta
from collections import deque, defaultdict
import statistics


class SystemState(Enum):
    """System health states"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    CRITICAL = "critical"


class ThrottleLevel(Enum):
    """Throttling levels"""
    NONE = 0
    LIGHT = 25  # 25% reduction
    MODERATE = 50  # 50% reduction
    HEAVY = 75  # 75% reduction
    SEVERE = 90  # 90% reduction


@dataclass
class SystemMetrics:
    """System metrics snapshot"""
    timestamp: float
    memory_usage_percent: float
    cpu_usage_percent: float
    queue_sizes: Dict[str, int]
    processing_rates: Dict[str, float]
    error_rates: Dict[str, float]
    latency_p95: float


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for an exchange"""
    exchange: str
    state: str = "closed"  # closed, open, half_open
    failure_count: int = 0
    failure_threshold: int = 5
    timeout: float = 60.0
    last_failure_time: float = 0
    success_count: int = 0
    half_open_success_threshold: int = 3


class BackpressureController:
    """Controls system backpressure and implements circuit breaker pattern"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="backpressure_controller")

        # System monitoring
        self.current_state = SystemState.HEALTHY
        self.throttle_level = ThrottleLevel.NONE
        self.metrics_history: deque[SystemMetrics] = deque(maxlen=300)  # 5 min at 1s intervals

        # Circuit breakers for exchanges
        self.circuit_breakers: Dict[str, CircuitBreakerState] = {}

        # Thresholds
        self.thresholds = {
            'memory_warning': 70.0,  # %
            'memory_critical': 85.0,  # %
            'cpu_warning': 80.0,  # %
            'cpu_critical': 90.0,  # %
            'queue_warning': 5000,  # items
            'queue_critical': 10000,  # items
            'error_rate_warning': 5.0,  # %
            'error_rate_critical': 15.0,  # %
            'latency_warning': 1000.0,  # ms
            'latency_critical': 5000.0  # ms
        }

        # Throttling callbacks
        self.throttle_callbacks: List[Callable] = []

        # Performance profiles
        self.performance_profiles = {
            'light': {
                'buffer_flush_interval': 100,  # ms
                'max_concurrent_tasks': 1000,
                'batch_sizes': {'ticker': 50, 'orderbook': 25, 'klines': 10}
            },
            'medium': {
                'buffer_flush_interval': 200,  # ms
                'max_concurrent_tasks': 500,
                'batch_sizes': {'ticker': 25, 'orderbook': 10, 'klines': 5}
            },
            'heavy': {
                'buffer_flush_interval': 500,  # ms
                'max_concurrent_tasks': 200,
                'batch_sizes': {'ticker': 10, 'orderbook': 5, 'klines': 2}
            }
        }

        self.current_profile = 'light'

        # State
        self.running = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.metrics_task: Optional[asyncio.Task] = None

        # Rate limiting
        self.exchange_rates: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.throttled_exchanges: Set[str] = set()

    async def initialize(self):
        """Initialize backpressure controller"""
        self.running = True

        # Start monitoring tasks
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.metrics_task = asyncio.create_task(self._metrics_collection_loop())

        self.logger.info("Backpressure controller initialized")

    async def shutdown(self):
        """Shutdown backpressure controller"""
        self.running = False

        # Cancel tasks
        for task in [self.monitoring_task, self.metrics_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.logger.info("Backpressure controller shutdown complete")

    def register_throttle_callback(self, callback: Callable):
        """Register callback for throttling notifications"""
        self.throttle_callbacks.append(callback)

    async def check_should_throttle(self, exchange: str, data_type: str) -> bool:
        """Check if input should be throttled"""
        # Check circuit breaker
        if not self._is_circuit_closed(exchange):
            return True

        # Check system state
        if self.current_state in [SystemState.OVERLOADED, SystemState.CRITICAL]:
            return True

        # Check exchange-specific throttling
        if exchange in self.throttled_exchanges:
            return True

        # Apply throttle level
        if self.throttle_level != ThrottleLevel.NONE:
            # Simple probabilistic throttling
            import random
            return random.random() * 100 < self.throttle_level.value

        return False

    async def record_processing_success(self, exchange: str, processing_time: float):
        """Record successful processing"""
        current_time = time.time()
        self.exchange_rates[exchange].append((current_time, processing_time, True))

        # Update circuit breaker
        self._record_circuit_success(exchange)

    async def record_processing_failure(self, exchange: str, error: str):
        """Record processing failure"""
        current_time = time.time()
        self.exchange_rates[exchange].append((current_time, 0, False))

        # Update circuit breaker
        self._record_circuit_failure(exchange, error)

    def _is_circuit_closed(self, exchange: str) -> bool:
        """Check if circuit breaker is closed (allowing requests)"""
        if exchange not in self.circuit_breakers:
            self.circuit_breakers[exchange] = CircuitBreakerState(exchange=exchange)

        breaker = self.circuit_breakers[exchange]
        current_time = time.time()

        if breaker.state == "closed":
            return True
        elif breaker.state == "open":
            # Check if timeout has passed
            if current_time - breaker.last_failure_time > breaker.timeout:
                breaker.state = "half_open"
                breaker.success_count = 0
                self.logger.info("Circuit breaker half-open", exchange=exchange)
                return True
            return False
        else:  # half_open
            return True

    def _record_circuit_success(self, exchange: str):
        """Record successful operation for circuit breaker"""
        if exchange not in self.circuit_breakers:
            return

        breaker = self.circuit_breakers[exchange]

        if breaker.state == "half_open":
            breaker.success_count += 1
            if breaker.success_count >= breaker.half_open_success_threshold:
                breaker.state = "closed"
                breaker.failure_count = 0
                self.logger.info("Circuit breaker closed", exchange=exchange)
        elif breaker.state == "closed":
            # Reset failure count on success
            breaker.failure_count = max(0, breaker.failure_count - 1)

    def _record_circuit_failure(self, exchange: str, error: str):
        """Record failed operation for circuit breaker"""
        if exchange not in self.circuit_breakers:
            self.circuit_breakers[exchange] = CircuitBreakerState(exchange=exchange)

        breaker = self.circuit_breakers[exchange]
        breaker.failure_count += 1
        breaker.last_failure_time = time.time()

        if breaker.failure_count >= breaker.failure_threshold:
            breaker.state = "open"
            self.logger.warning("Circuit breaker opened",
                                exchange=exchange,
                                failure_count=breaker.failure_count,
                                error=error)

            # Add to throttled exchanges
            self.throttled_exchanges.add(exchange)

    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                await asyncio.sleep(1.0)  # Monitor every second
                await self._evaluate_system_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Monitoring loop error", error=str(e))
                await asyncio.sleep(5.0)

    async def _metrics_collection_loop(self):
        """Collect system metrics"""
        while self.running:
            try:
                await asyncio.sleep(1.0)
                await self._collect_metrics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Metrics collection error", error=str(e))
                await asyncio.sleep(5.0)

    async def _collect_metrics(self):
        """Collect current system metrics"""
        try:
            # System metrics
            memory_percent = psutil.virtual_memory().percent
            cpu_percent = psutil.cpu_percent()

            # Queue metrics (integrate with your task queue)
            queue_sizes = {}
            try:
                from .task_queue import task_queue
                stats = task_queue.get_stats()
                for worker_type, queue_stats in stats.get('queue_stats', {}).items():
                    queue_sizes[worker_type] = queue_stats.get('queue_size', 0)
            except:
                queue_sizes = {'fast': 0, 'medium': 0, 'slow': 0}

            # Calculate processing rates and error rates
            processing_rates = {}
            error_rates = {}

            for exchange, rates in self.exchange_rates.items():
                if rates:
                    # Filter last minute
                    current_time = time.time()
                    recent_rates = [r for r in rates if current_time - r[0] <= 60]

                    if recent_rates:
                        total_ops = len(recent_rates)
                        successful_ops = sum(1 for r in recent_rates if r[2])
                        failed_ops = total_ops - successful_ops

                        processing_rates[exchange] = total_ops / 60.0  # ops per second
                        error_rates[exchange] = (failed_ops / total_ops) * 100 if total_ops > 0 else 0
                    else:
                        processing_rates[exchange] = 0
                        error_rates[exchange] = 0

            # Calculate latency P95
            all_latencies = []
            for rates in self.exchange_rates.values():
                current_time = time.time()
                recent_rates = [r for r in rates if current_time - r[0] <= 60 and r[2]]  # Only successful
                all_latencies.extend([r[1] * 1000 for r in recent_rates])  # Convert to ms

            latency_p95 = statistics.quantiles(all_latencies, n=20)[18] if len(all_latencies) > 10 else 0

            # Create metrics snapshot
            metrics = SystemMetrics(
                timestamp=time.time(),
                memory_usage_percent=memory_percent,
                cpu_usage_percent=cpu_percent,
                queue_sizes=queue_sizes,
                processing_rates=processing_rates,
                error_rates=error_rates,
                latency_p95=latency_p95
            )

            self.metrics_history.append(metrics)

        except Exception as e:
            self.logger.error("Failed to collect metrics", error=str(e))

    async def _evaluate_system_state(self):
        """Evaluate current system state and apply controls"""
        if not self.metrics_history:
            return

        latest_metrics = self.metrics_history[-1]

        # Determine system state
        old_state = self.current_state
        new_state = self._calculate_system_state(latest_metrics)

        if new_state != old_state:
            self.current_state = new_state
            await self._on_state_change(old_state, new_state)

        # Update throttle level
        old_throttle = self.throttle_level
        new_throttle = self._calculate_throttle_level(latest_metrics)

        if new_throttle != old_throttle:
            self.throttle_level = new_throttle
            await self._on_throttle_change(old_throttle, new_throttle)

        # Update performance profile
        await self._update_performance_profile()

        # Clean up throttled exchanges
        await self._cleanup_throttled_exchanges()

    def _calculate_system_state(self, metrics: SystemMetrics) -> SystemState:
        """Calculate system state based on metrics"""
        critical_conditions = 0
        warning_conditions = 0

        # Memory check
        if metrics.memory_usage_percent > self.thresholds['memory_critical']:
            critical_conditions += 1
        elif metrics.memory_usage_percent > self.thresholds['memory_warning']:
            warning_conditions += 1

        # CPU check
        if metrics.cpu_usage_percent > self.thresholds['cpu_critical']:
            critical_conditions += 1
        elif metrics.cpu_usage_percent > self.thresholds['cpu_warning']:
            warning_conditions += 1

        # Queue check
        max_queue_size = max(metrics.queue_sizes.values()) if metrics.queue_sizes else 0
        if max_queue_size > self.thresholds['queue_critical']:
            critical_conditions += 1
        elif max_queue_size > self.thresholds['queue_warning']:
            warning_conditions += 1

        # Error rate check
        max_error_rate = max(metrics.error_rates.values()) if metrics.error_rates else 0
        if max_error_rate > self.thresholds['error_rate_critical']:
            critical_conditions += 1
        elif max_error_rate > self.thresholds['error_rate_warning']:
            warning_conditions += 1

        # Latency check
        if metrics.latency_p95 > self.thresholds['latency_critical']:
            critical_conditions += 1
        elif metrics.latency_p95 > self.thresholds['latency_warning']:
            warning_conditions += 1

        # Determine state
        if critical_conditions >= 2:
            return SystemState.CRITICAL
        elif critical_conditions >= 1 or warning_conditions >= 3:
            return SystemState.OVERLOADED
        elif warning_conditions >= 1:
            return SystemState.DEGRADED
        else:
            return SystemState.HEALTHY

    def _calculate_throttle_level(self, metrics: SystemMetrics) -> ThrottleLevel:
        """Calculate appropriate throttle level"""
        if self.current_state == SystemState.CRITICAL:
            return ThrottleLevel.SEVERE
        elif self.current_state == SystemState.OVERLOADED:
            # Check specific metrics for fine-tuning
            if metrics.memory_usage_percent > 90 or max(metrics.queue_sizes.values(), default=0) > 15000:
                return ThrottleLevel.HEAVY
            else:
                return ThrottleLevel.MODERATE
        elif self.current_state == SystemState.DEGRADED:
            return ThrottleLevel.LIGHT
        else:
            return ThrottleLevel.NONE

    async def _on_state_change(self, old_state: SystemState, new_state: SystemState):
        """Handle system state change"""
        self.logger.info("System state changed",
                         old_state=old_state.value, new_state=new_state.value)

        # Notify callbacks
        for callback in self.throttle_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback('state_change', old_state.value, new_state.value)
                else:
                    callback('state_change', old_state.value, new_state.value)
            except Exception as e:
                self.logger.error("State change callback failed", error=str(e))

    async def _on_throttle_change(self, old_throttle: ThrottleLevel, new_throttle: ThrottleLevel):
        """Handle throttle level change"""
        self.logger.info("Throttle level changed",
                         old_level=old_throttle.value, new_level=new_throttle.value)

        # Notify callbacks
        for callback in self.throttle_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback('throttle_change', old_throttle.value, new_throttle.value)
                else:
                    callback('throttle_change', old_throttle.value, new_throttle.value)
            except Exception as e:
                self.logger.error("Throttle change callback failed", error=str(e))

    async def _update_performance_profile(self):
        """Update performance profile based on system state"""
        if self.current_state == SystemState.CRITICAL:
            new_profile = 'heavy'
        elif self.current_state == SystemState.OVERLOADED:
            new_profile = 'medium'
        else:
            new_profile = 'light'

        if new_profile != self.current_profile:
            self.current_profile = new_profile
            self.logger.info("Performance profile changed", profile=new_profile)

    async def _cleanup_throttled_exchanges(self):
        """Clean up throttled exchanges that have recovered"""
        current_time = time.time()
        exchanges_to_remove = set()

        for exchange in self.throttled_exchanges:
            if exchange in self.circuit_breakers:
                breaker = self.circuit_breakers[exchange]
                # Remove from throttled if circuit is closed and some time has passed
                if (breaker.state == "closed" and
                        current_time - breaker.last_failure_time > 300):  # 5 minutes
                    exchanges_to_remove.add(exchange)

        for exchange in exchanges_to_remove:
            self.throttled_exchanges.remove(exchange)
            self.logger.info("Exchange removed from throttling", exchange=exchange)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive backpressure statistics"""
        latest_metrics = self.metrics_history[-1] if self.metrics_history else None

        circuit_breaker_stats = {}
        for exchange, breaker in self.circuit_breakers.items():
            circuit_breaker_stats[exchange] = {
                'state': breaker.state,
                'failure_count': breaker.failure_count,
                'success_count': breaker.success_count,
                'last_failure_time': breaker.last_failure_time
            }

        return {
            'system_state': self.current_state.value,
            'throttle_level': self.throttle_level.value,
            'performance_profile': self.current_profile,
            'throttled_exchanges': list(self.throttled_exchanges),
            'circuit_breakers': circuit_breaker_stats,
            'latest_metrics': {
                'memory_usage_percent': latest_metrics.memory_usage_percent if latest_metrics else 0,
                'cpu_usage_percent': latest_metrics.cpu_usage_percent if latest_metrics else 0,
                'queue_sizes': latest_metrics.queue_sizes if latest_metrics else {},
                'processing_rates': latest_metrics.processing_rates if latest_metrics else {},
                'error_rates': latest_metrics.error_rates if latest_metrics else {},
                'latency_p95': latest_metrics.latency_p95 if latest_metrics else 0
            },
            'thresholds': self.thresholds,
            'running': self.running
        }

    def get_performance_config(self) -> Dict[str, Any]:
        """Get current performance configuration"""
        return self.performance_profiles[self.current_profile].copy()


# Global instance
backpressure_controller = BackpressureController()

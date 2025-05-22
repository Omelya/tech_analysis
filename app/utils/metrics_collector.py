import time
from typing import Dict, Any, Optional, List
from collections import defaultdict, deque
import structlog
from datetime import datetime, timedelta
import asyncio
import json


class MetricsCollector:
    """Collect and manage application metrics"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="metrics_collector")
        self.metrics = defaultdict(lambda: defaultdict(int))
        self.timeseries_metrics = defaultdict(lambda: deque(maxlen=1000))
        self.start_time = time.time()

        # Performance tracking
        self.request_times = defaultdict(lambda: deque(maxlen=100))
        self.error_counts = defaultdict(int)
        self.success_counts = defaultdict(int)

        # Exchange specific metrics
        self.exchange_metrics = defaultdict(lambda: {
            'requests_sent': 0,
            'responses_received': 0,
            'errors': 0,
            'rate_limit_hits': 0,
            'avg_response_time': 0,
            'last_request': None
        })

    def increment_counter(self, metric_name: str, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric"""
        key = self._build_metric_key(metric_name, labels)
        self.metrics['counters'][key] += 1

    def set_gauge(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric value"""
        key = self._build_metric_key(metric_name, labels)
        self.metrics['gauges'][key] = value

    def record_histogram(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a histogram value"""
        key = self._build_metric_key(metric_name, labels)
        self.timeseries_metrics[key].append({
            'value': value,
            'timestamp': time.time()
        })

    def time_operation(self, operation_name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager for timing operations"""
        return OperationTimer(self, operation_name, labels)

    def record_api_request(self, exchange: str, endpoint: str, success: bool,
                           response_time: float, error: Optional[str] = None):
        """Record API request metrics"""
        exchange_data = self.exchange_metrics[exchange]
        exchange_data['requests_sent'] += 1
        exchange_data['last_request'] = datetime.now().isoformat()

        if success:
            exchange_data['responses_received'] += 1
            self.success_counts[f"{exchange}_{endpoint}"] += 1
        else:
            exchange_data['errors'] += 1
            self.error_counts[f"{exchange}_{endpoint}"] += 1

            if error and 'rate limit' in error.lower():
                exchange_data['rate_limit_hits'] += 1

        # Update average response time
        self.request_times[f"{exchange}_{endpoint}"].append(response_time)
        times = list(self.request_times[f"{exchange}_{endpoint}"])
        exchange_data['avg_response_time'] = sum(times) / len(times) if times else 0

        # Record detailed metrics
        self.record_histogram(
            'api_request_duration',
            response_time,
            {'exchange': exchange, 'endpoint': endpoint}
        )

        self.increment_counter(
            'api_requests_total',
            {'exchange': exchange, 'endpoint': endpoint, 'status': 'success' if success else 'error'}
        )

    def record_data_processing(self, data_type: str, exchange: str, symbol: str,
                               count: int, processing_time: float):
        """Record data processing metrics"""
        self.increment_counter(
            'data_processed_total',
            {'type': data_type, 'exchange': exchange, 'symbol': symbol}
        )

        self.set_gauge(
            'data_processing_count',
            count,
            {'type': data_type, 'exchange': exchange}
        )

        self.record_histogram(
            'data_processing_duration',
            processing_time,
            {'type': data_type, 'exchange': exchange}
        )

    def record_database_operation(self, operation: str, success: bool, duration: float):
        """Record database operation metrics"""
        self.increment_counter(
            'database_operations_total',
            {'operation': operation, 'status': 'success' if success else 'error'}
        )

        self.record_histogram(
            'database_operation_duration',
            duration,
            {'operation': operation}
        )

    def record_cache_operation(self, operation: str, hit: bool):
        """Record cache operation metrics"""
        self.increment_counter(
            'cache_operations_total',
            {'operation': operation, 'result': 'hit' if hit else 'miss'}
        )

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        uptime = time.time() - self.start_time

        summary = {
            'uptime_seconds': uptime,
            'timestamp': datetime.now().isoformat(),
            'counters': dict(self.metrics['counters']),
            'gauges': dict(self.metrics['gauges']),
            'exchanges': dict(self.exchange_metrics),
            'system': {
                'total_errors': sum(self.error_counts.values()),
                'total_successes': sum(self.success_counts.values()),
                'error_rate': self._calculate_error_rate()
            }
        }

        return summary

    def get_exchange_metrics(self, exchange: str) -> Dict[str, Any]:
        """Get metrics for specific exchange"""
        if exchange not in self.exchange_metrics:
            return {}

        metrics = dict(self.exchange_metrics[exchange])

        # Add calculated metrics
        total_requests = metrics['requests_sent']
        if total_requests > 0:
            metrics['success_rate'] = (metrics['responses_received'] / total_requests) * 100
            metrics['error_rate'] = (metrics['errors'] / total_requests) * 100
        else:
            metrics['success_rate'] = 0
            metrics['error_rate'] = 0

        return metrics

    def get_timeseries_data(self, metric_name: str, labels: Optional[Dict[str, str]] = None,
                            minutes: int = 60) -> List[Dict[str, Any]]:
        """Get timeseries data for a metric"""
        key = self._build_metric_key(metric_name, labels)

        if key not in self.timeseries_metrics:
            return []

        cutoff_time = time.time() - (minutes * 60)
        data = []

        for point in self.timeseries_metrics[key]:
            if point['timestamp'] >= cutoff_time:
                data.append(point)

        return data

    def reset_metrics(self):
        """Reset all metrics (useful for testing)"""
        self.metrics.clear()
        self.timeseries_metrics.clear()
        self.request_times.clear()
        self.error_counts.clear()
        self.success_counts.clear()
        self.exchange_metrics.clear()
        self.start_time = time.time()

    def _build_metric_key(self, metric_name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Build metric key with labels"""
        if not labels:
            return metric_name

        label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{metric_name}{{{label_str}}}"

    def _calculate_error_rate(self) -> float:
        """Calculate overall error rate"""
        total_errors = sum(self.error_counts.values())
        total_successes = sum(self.success_counts.values())
        total_requests = total_errors + total_successes

        if total_requests == 0:
            return 0.0

        return (total_errors / total_requests) * 100

    async def start_periodic_logging(self, interval: int = 300):
        """Start periodic metrics logging"""
        while True:
            try:
                summary = self.get_metrics_summary()
                self.logger.info("Metrics summary", metrics=summary)
                await asyncio.sleep(interval)
            except Exception as e:
                self.logger.error("Error in periodic metrics logging", error=str(e))
                await asyncio.sleep(interval)


class OperationTimer:
    """Context manager for timing operations"""

    def __init__(self, collector: MetricsCollector, operation_name: str,
                 labels: Optional[Dict[str, str]] = None):
        self.collector = collector
        self.operation_name = operation_name
        self.labels = labels
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.collector.record_histogram(self.operation_name, duration, self.labels)

            # Record success/failure
            success = exc_type is None
            self.collector.increment_counter(
                f"{self.operation_name}_total",
                {**self.labels, 'status': 'success' if success else 'error'} if self.labels else {
                    'status': 'success' if success else 'error'}
            )

    async def __aenter__(self):
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.collector.record_histogram(self.operation_name, duration, self.labels)

            # Record success/failure
            success = exc_type is None
            self.collector.increment_counter(
                f"{self.operation_name}_total",
                {**self.labels, 'status': 'success' if success else 'error'} if self.labels else {
                    'status': 'success' if success else 'error'}
            )


# Global instance
metrics_collector = MetricsCollector()

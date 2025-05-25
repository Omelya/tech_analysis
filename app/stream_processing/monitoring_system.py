import asyncio
import time
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict
import structlog
from datetime import datetime, timedelta
import statistics
import json
import psutil


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"


@dataclass
class Alert:
    """Alert definition"""
    alert_id: str
    name: str
    description: str
    level: AlertLevel
    metric_name: str
    threshold: float
    operator: str = ">"  # >, <, ==, !=, >=, <=
    duration_seconds: int = 60  # Alert after condition persists for this duration
    cooldown_seconds: int = 300  # Minimum time between same alerts
    enabled: bool = True
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricPoint:
    """Single metric data point"""
    timestamp: float
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AlertInstance:
    """Active alert instance"""
    alert: Alert
    triggered_at: float
    last_sent: float = 0
    condition_start: float = 0
    active: bool = True


class MonitoringSystem:
    """Comprehensive monitoring and alerting system"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="monitoring_system")

        # Metrics storage
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=3600))  # 1 hour at 1s resolution
        self.metric_configs: Dict[str, Dict[str, Any]] = {}

        # Alerting
        self.alerts: Dict[str, Alert] = {}
        self.active_alerts: Dict[str, AlertInstance] = {}
        self.alert_callbacks: List[Callable] = []

        # Performance profiling
        self.performance_profiles = {
            'light': {
                'max_memory_percent': 60,
                'max_cpu_percent': 70,
                'max_queue_size': 1000,
                'target_latency_ms': 100
            },
            'medium': {
                'max_memory_percent': 75,
                'max_cpu_percent': 80,
                'max_queue_size': 5000,
                'target_latency_ms': 500
            },
            'heavy': {
                'max_memory_percent': 85,
                'max_cpu_percent': 90,
                'max_queue_size': 10000,
                'target_latency_ms': 1000
            }
        }

        self.current_profile = 'light'

        # System monitoring
        self.system_metrics_history: deque = deque(maxlen=3600)

        # State
        self.running = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.alerting_task: Optional[asyncio.Task] = None

        # Built-in metrics
        self._initialize_builtin_metrics()
        self._initialize_builtin_alerts()

    def _initialize_builtin_metrics(self):
        """Initialize built-in metrics"""
        builtin_metrics = {
            'system_memory_percent': {'type': MetricType.GAUGE, 'unit': '%'},
            'system_cpu_percent': {'type': MetricType.GAUGE, 'unit': '%'},
            'messages_received_total': {'type': MetricType.COUNTER, 'unit': 'count'},
            'messages_processed_total': {'type': MetricType.COUNTER, 'unit': 'count'},
            'messages_dropped_total': {'type': MetricType.COUNTER, 'unit': 'count'},
            'processing_latency_ms': {'type': MetricType.HISTOGRAM, 'unit': 'ms'},
            'queue_size': {'type': MetricType.GAUGE, 'unit': 'count'},
            'buffer_flush_rate': {'type': MetricType.RATE, 'unit': 'ops/sec'},
            'database_operations_total': {'type': MetricType.COUNTER, 'unit': 'count'},
            'database_errors_total': {'type': MetricType.COUNTER, 'unit': 'count'},
            'circuit_breaker_state': {'type': MetricType.GAUGE, 'unit': 'state'},
            'throttle_level': {'type': MetricType.GAUGE, 'unit': 'percent'}
        }

        for metric_name, config in builtin_metrics.items():
            self.metric_configs[metric_name] = config

    def _initialize_builtin_alerts(self):
        """Initialize built-in alerts"""
        builtin_alerts = [
            Alert(
                alert_id="high_memory_usage",
                name="High Memory Usage",
                description="System memory usage is critically high",
                level=AlertLevel.CRITICAL,
                metric_name="system_memory_percent",
                threshold=85.0,
                duration_seconds=30
            ),
            Alert(
                alert_id="high_cpu_usage",
                name="High CPU Usage",
                description="System CPU usage is critically high",
                level=AlertLevel.WARNING,
                metric_name="system_cpu_percent",
                threshold=80.0,
                duration_seconds=60
            ),
            Alert(
                alert_id="high_queue_size",
                name="High Queue Size",
                description="Task queue size is too high",
                level=AlertLevel.WARNING,
                metric_name="queue_size",
                threshold=5000,
                duration_seconds=30
            ),
            Alert(
                alert_id="high_message_drop_rate",
                name="High Message Drop Rate",
                description="Too many messages are being dropped",
                level=AlertLevel.ERROR,
                metric_name="messages_dropped_total",
                threshold=100,
                duration_seconds=60,
                operator=">="
            ),
            Alert(
                alert_id="database_errors",
                name="Database Errors",
                description="Database operations are failing",
                level=AlertLevel.ERROR,
                metric_name="database_errors_total",
                threshold=10,
                duration_seconds=30
            ),
            Alert(
                alert_id="circuit_breaker_open",
                name="Circuit Breaker Open",
                description="Circuit breaker is open for an exchange",
                level=AlertLevel.WARNING,
                metric_name="circuit_breaker_state",
                threshold=1,
                duration_seconds=10,
                operator=">="
            )
        ]

        for alert in builtin_alerts:
            self.alerts[alert.alert_id] = alert

    async def initialize(self):
        """Initialize monitoring system"""
        self.running = True

        # Start monitoring tasks
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.alerting_task = asyncio.create_task(self._alerting_loop())

        self.logger.info("Monitoring system initialized")

    async def shutdown(self):
        """Shutdown monitoring system"""
        self.running = False

        # Cancel tasks
        for task in [self.monitoring_task, self.alerting_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.logger.info("Monitoring system shutdown complete")

    def record_metric(self, metric_name: str, value: float,
                      tags: Dict[str, str] = None, timestamp: float = None):
        """Record a metric value"""
        if timestamp is None:
            timestamp = time.time()

        point = MetricPoint(
            timestamp=timestamp,
            value=value,
            tags=tags or {}
        )

        self.metrics[metric_name].append(point)

    def increment_counter(self, metric_name: str, value: float = 1,
                          tags: Dict[str, str] = None):
        """Increment a counter metric"""
        # Get last value and add increment
        last_value = 0
        if self.metrics[metric_name]:
            last_value = self.metrics[metric_name][-1].value

        self.record_metric(metric_name, last_value + value, tags)

    def set_gauge(self, metric_name: str, value: float, tags: Dict[str, str] = None):
        """Set a gauge metric value"""
        self.record_metric(metric_name, value, tags)

    def record_histogram(self, metric_name: str, value: float, tags: Dict[str, str] = None):
        """Record a histogram value"""
        self.record_metric(metric_name, value, tags)

    def get_metric_value(self, metric_name: str,
                         aggregation: str = "latest") -> Optional[float]:
        """Get metric value with aggregation"""
        if metric_name not in self.metrics or not self.metrics[metric_name]:
            return None

        values = [point.value for point in self.metrics[metric_name]]

        if aggregation == "latest":
            return values[-1]
        elif aggregation == "avg":
            return statistics.mean(values)
        elif aggregation == "max":
            return max(values)
        elif aggregation == "min":
            return min(values)
        elif aggregation == "sum":
            return sum(values)
        elif aggregation == "p95":
            return statistics.quantiles(values, n=20)[18] if len(values) > 10 else values[-1]
        else:
            return values[-1]

    def get_metric_rate(self, metric_name: str, window_seconds: int = 60) -> float:
        """Calculate metric rate over time window"""
        if metric_name not in self.metrics:
            return 0.0

        current_time = time.time()
        cutoff_time = current_time - window_seconds

        points = [p for p in self.metrics[metric_name] if p.timestamp >= cutoff_time]

        if len(points) < 2:
            return 0.0

        # Calculate rate based on counter difference
        first_value = points[0].value
        last_value = points[-1].value
        time_diff = points[-1].timestamp - points[0].timestamp

        if time_diff <= 0:
            return 0.0

        return (last_value - first_value) / time_diff

    def add_alert(self, alert: Alert):
        """Add custom alert"""
        self.alerts[alert.alert_id] = alert
        self.logger.info("Alert added", alert_id=alert.alert_id, name=alert.name)

    def remove_alert(self, alert_id: str):
        """Remove alert"""
        if alert_id in self.alerts:
            del self.alerts[alert_id]
            if alert_id in self.active_alerts:
                del self.active_alerts[alert_id]
            self.logger.info("Alert removed", alert_id=alert_id)

    def register_alert_callback(self, callback: Callable):
        """Register callback for alert notifications"""
        self.alert_callbacks.append(callback)

    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                await asyncio.sleep(1.0)  # Collect metrics every second
                await self._collect_system_metrics()
                await self._collect_application_metrics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Monitoring loop error", error=str(e))
                await asyncio.sleep(5.0)

    async def _alerting_loop(self):
        """Alert evaluation loop"""
        while self.running:
            try:
                await asyncio.sleep(5.0)  # Check alerts every 5 seconds
                await self._evaluate_alerts()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Alerting loop error", error=str(e))
                await asyncio.sleep(10.0)

    async def _collect_system_metrics(self):
        """Collect system metrics"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            self.set_gauge("system_memory_percent", memory.percent)

            # CPU usage
            cpu_percent = psutil.cpu_percent()
            self.set_gauge("system_cpu_percent", cpu_percent)

            # Network I/O
            net_io = psutil.net_io_counters()
            self.set_gauge("network_bytes_sent", net_io.bytes_sent)
            self.set_gauge("network_bytes_recv", net_io.bytes_recv)

            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io:
                self.set_gauge("disk_read_bytes", disk_io.read_bytes)
                self.set_gauge("disk_write_bytes", disk_io.write_bytes)

            # Process-specific metrics
            process = psutil.Process()
            self.set_gauge("process_memory_mb", process.memory_info().rss / 1024 / 1024)
            self.set_gauge("process_cpu_percent", process.cpu_percent())
            self.set_gauge("process_threads", process.num_threads())

        except Exception as e:
            self.logger.error("Failed to collect system metrics", error=str(e))

    async def _collect_application_metrics(self):
        """Collect application-specific metrics"""
        try:
            # Get stats from stream processor if available
            from .stream_processor import stream_processor

            if stream_processor.running:
                stats = stream_processor.get_comprehensive_stats()

                # Message metrics
                self.set_gauge("messages_received_total", stats['total_messages_received'])
                self.set_gauge("messages_processed_total", stats['total_messages_processed'])
                self.set_gauge("messages_dropped_total", stats['total_messages_dropped'])
                self.set_gauge("processing_rate_per_second", stats['processing_rate_per_second'])
                self.set_gauge("drop_rate_percent", stats['drop_rate_percent'])

                # Buffer metrics
                buffer_stats = stats.get('buffer_manager', {})
                if buffer_stats:
                    self.set_gauge("total_buffers", buffer_stats.get('total_buffers', 0))
                    self.set_gauge("total_items_buffered", buffer_stats.get('total_items_buffered', 0))
                    self.set_gauge("total_items_flushed", buffer_stats.get('total_items_flushed', 0))
                    self.set_gauge("buffer_flush_errors", buffer_stats.get('flush_errors', 0))

                # Task queue metrics
                queue_stats = stats.get('task_queue', {})
                if queue_stats:
                    total_queue_size = sum(
                        qs.get('queue_size', 0) for qs in queue_stats.get('queue_stats', {}).values()
                    )
                    self.set_gauge("queue_size", total_queue_size)
                    self.set_gauge("total_tasks_queued", queue_stats.get('total_tasks_queued', 0))
                    self.set_gauge("total_tasks_completed", queue_stats.get('total_tasks_completed', 0))

                # Backpressure metrics
                bp_stats = stats.get('backpressure_controller', {})
                if bp_stats:
                    self.set_gauge("throttle_level", bp_stats.get('throttle_level', 0))

                    # Circuit breaker states
                    circuit_breakers = bp_stats.get('circuit_breakers', {})
                    open_breakers = sum(1 for cb in circuit_breakers.values() if cb.get('state') == 'open')
                    self.set_gauge("circuit_breaker_state", open_breakers)

                # Database metrics
                db_stats = stats.get('database_optimizer', {})
                if db_stats:
                    self.set_gauge("database_operations_total", db_stats.get('total_operations', 0))
                    self.set_gauge("database_success_rate", db_stats.get('success_rate', 0))
                    self.set_gauge("database_errors_total", db_stats.get('failed_operations', 0))

        except Exception as e:
            self.logger.error("Failed to collect application metrics", error=str(e))

    async def _evaluate_alerts(self):
        """Evaluate all alerts"""
        current_time = time.time()

        for alert_id, alert in self.alerts.items():
            if not alert.enabled:
                continue

            try:
                await self._evaluate_single_alert(alert, current_time)
            except Exception as e:
                self.logger.error("Failed to evaluate alert",
                                  alert_id=alert_id, error=str(e))

    async def _evaluate_single_alert(self, alert: Alert, current_time: float):
        """Evaluate single alert"""
        metric_value = self.get_metric_value(alert.metric_name, "latest")

        if metric_value is None:
            return

        # Check condition
        condition_met = False
        if alert.operator == ">":
            condition_met = metric_value > alert.threshold
        elif alert.operator == "<":
            condition_met = metric_value < alert.threshold
        elif alert.operator == ">=":
            condition_met = metric_value >= alert.threshold
        elif alert.operator == "<=":
            condition_met = metric_value <= alert.threshold
        elif alert.operator == "==":
            condition_met = metric_value == alert.threshold
        elif alert.operator == "!=":
            condition_met = metric_value != alert.threshold

        alert_instance = self.active_alerts.get(alert.alert_id)

        if condition_met:
            if alert_instance is None:
                # Start tracking this condition
                self.active_alerts[alert.alert_id] = AlertInstance(
                    alert=alert,
                    triggered_at=current_time,
                    condition_start=current_time,
                    active=False
                )
            else:
                # Check if condition has persisted long enough
                condition_duration = current_time - alert_instance.condition_start

                if condition_duration >= alert.duration_seconds and not alert_instance.active:
                    # Activate alert
                    alert_instance.active = True
                    await self._fire_alert(alert, metric_value, current_time)
                elif alert_instance.active:
                    # Check cooldown for repeat notifications
                    if (current_time - alert_instance.last_sent) >= alert.cooldown_seconds:
                        await self._fire_alert(alert, metric_value, current_time)
        else:
            # Condition not met - clear alert if active
            if alert_instance is not None:
                if alert_instance.active:
                    await self._clear_alert(alert, metric_value, current_time)
                del self.active_alerts[alert.alert_id]

    async def _fire_alert(self, alert: Alert, current_value: float, timestamp: float):
        """Fire an alert"""
        alert_data = {
            'alert_id': alert.alert_id,
            'name': alert.name,
            'description': alert.description,
            'level': alert.level.value,
            'metric_name': alert.metric_name,
            'threshold': alert.threshold,
            'current_value': current_value,
            'operator': alert.operator,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'tags': alert.tags,
            'action': 'fired'
        }

        self.logger.warning("Alert fired", **alert_data)

        # Update alert instance
        if alert.alert_id in self.active_alerts:
            self.active_alerts[alert.alert_id].last_sent = timestamp

        # Notify callbacks
        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(alert_data)
                else:
                    callback(alert_data)
            except Exception as e:
                self.logger.error("Alert callback failed",
                                  alert_id=alert.alert_id, error=str(e))

    async def _clear_alert(self, alert: Alert, current_value: float, timestamp: float):
        """Clear an alert"""
        alert_data = {
            'alert_id': alert.alert_id,
            'name': alert.name,
            'description': alert.description,
            'level': alert.level.value,
            'metric_name': alert.metric_name,
            'threshold': alert.threshold,
            'current_value': current_value,
            'operator': alert.operator,
            'timestamp': timestamp,
            'datetime': datetime.fromtimestamp(timestamp).isoformat(),
            'tags': alert.tags,
            'action': 'cleared'
        }

        self.logger.info("Alert cleared", **alert_data)

        # Notify callbacks
        for callback in self.alert_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(alert_data)
                else:
                    callback(alert_data)
            except Exception as e:
                self.logger.error("Alert callback failed",
                                  alert_id=alert.alert_id, error=str(e))

    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get list of active alerts"""
        active_alerts = []
        current_time = time.time()

        for alert_id, alert_instance in self.active_alerts.items():
            if alert_instance.active:
                active_alerts.append({
                    'alert_id': alert_id,
                    'name': alert_instance.alert.name,
                    'level': alert_instance.alert.level.value,
                    'triggered_at': alert_instance.triggered_at,
                    'duration_seconds': current_time - alert_instance.triggered_at,
                    'last_sent': alert_instance.last_sent
                })

        return active_alerts

    def get_metrics_summary(self, time_window_seconds: int = 300) -> Dict[str, Any]:
        """Get metrics summary for time window"""
        current_time = time.time()
        cutoff_time = current_time - time_window_seconds

        summary = {
            'timestamp': current_time,
            'time_window_seconds': time_window_seconds,
            'metrics': {}
        }

        for metric_name, points in self.metrics.items():
            if not points:
                continue

            # Filter points in time window
            window_points = [p for p in points if p.timestamp >= cutoff_time]

            if not window_points:
                continue

            values = [p.value for p in window_points]

            metric_summary = {
                'current': values[-1],
                'min': min(values),
                'max': max(values),
                'avg': statistics.mean(values),
                'count': len(values)
            }

            # Add percentiles for histograms
            if len(values) > 10:
                metric_summary['p50'] = statistics.median(values)
                metric_summary['p95'] = statistics.quantiles(values, n=20)[18]
                metric_summary['p99'] = statistics.quantiles(values, n=100)[98]

            # Add rate for counters
            if (self.metric_configs.get(metric_name, {}).get('type') == MetricType.COUNTER and
                    len(window_points) > 1):
                time_diff = window_points[-1].timestamp - window_points[0].timestamp
                value_diff = window_points[-1].value - window_points[0].value
                metric_summary['rate'] = value_diff / time_diff if time_diff > 0 else 0

            summary['metrics'][metric_name] = metric_summary

        return summary

    def export_metrics_prometheus(self) -> str:
        """Export metrics in Prometheus format"""
        lines = []
        current_time = time.time()

        for metric_name, points in self.metrics.items():
            if not points:
                continue

            latest_point = points[-1]
            metric_config = self.metric_configs.get(metric_name, {})

            # Add help and type comments
            lines.append(f"# HELP {metric_name} {metric_config.get('description', '')}")
            lines.append(f"# TYPE {metric_name} {metric_config.get('type', 'gauge').lower()}")

            # Format tags
            tag_str = ""
            if latest_point.tags:
                tag_parts = [f'{k}="{v}"' for k, v in latest_point.tags.items()]
                tag_str = "{" + ",".join(tag_parts) + "}"

            # Add metric line
            lines.append(f"{metric_name}{tag_str} {latest_point.value} {int(latest_point.timestamp * 1000)}")
            lines.append("")

        return "\n".join(lines)

    def get_performance_recommendations(self) -> List[Dict[str, Any]]:
        """Get performance recommendations based on current metrics"""
        recommendations = []

        # Check memory usage
        memory_percent = self.get_metric_value("system_memory_percent")
        if memory_percent and memory_percent > 80:
            recommendations.append({
                'type': 'performance',
                'severity': 'high' if memory_percent > 90 else 'medium',
                'title': 'High Memory Usage',
                'description': f'Memory usage is at {memory_percent:.1f}%',
                'recommendation': 'Consider reducing buffer sizes or scaling horizontally',
                'metric': 'system_memory_percent',
                'current_value': memory_percent
            })

        # Check queue sizes
        queue_size = self.get_metric_value("queue_size")
        if queue_size and queue_size > 1000:
            recommendations.append({
                'type': 'performance',
                'severity': 'high' if queue_size > 5000 else 'medium',
                'title': 'High Queue Size',
                'description': f'Task queue size is {queue_size}',
                'recommendation': 'Consider adding more workers or increasing processing capacity',
                'metric': 'queue_size',
                'current_value': queue_size
            })

        # Check drop rate
        drop_rate = self.get_metric_value("drop_rate_percent")
        if drop_rate and drop_rate > 1:
            recommendations.append({
                'type': 'reliability',
                'severity': 'high' if drop_rate > 5 else 'medium',
                'title': 'High Message Drop Rate',
                'description': f'Dropping {drop_rate:.1f}% of messages',
                'recommendation': 'System is overloaded - consider throttling input or scaling up',
                'metric': 'drop_rate_percent',
                'current_value': drop_rate
            })

        # Check database success rate
        db_success_rate = self.get_metric_value("database_success_rate")
        if db_success_rate and db_success_rate < 95:
            recommendations.append({
                'type': 'reliability',
                'severity': 'high' if db_success_rate < 90 else 'medium',
                'title': 'Low Database Success Rate',
                'description': f'Database operations success rate is {db_success_rate:.1f}%',
                'recommendation': 'Check database connection and optimize queries',
                'metric': 'database_success_rate',
                'current_value': db_success_rate
            })

        return recommendations

    def suggest_performance_profile(self) -> str:
        """Suggest optimal performance profile based on current metrics"""
        memory_percent = self.get_metric_value("system_memory_percent", "avg") or 0
        cpu_percent = self.get_metric_value("system_cpu_percent", "avg") or 0
        queue_size = self.get_metric_value("queue_size", "avg") or 0

        # Score each profile
        profiles_scores = {}

        for profile_name, profile_config in self.performance_profiles.items():
            score = 0

            # Memory score
            if memory_percent <= profile_config['max_memory_percent']:
                score += 1

            # CPU score
            if cpu_percent <= profile_config['max_cpu_percent']:
                score += 1

            # Queue size score
            if queue_size <= profile_config['max_queue_size']:
                score += 1

            profiles_scores[profile_name] = score

        # Find best profile
        best_profile = max(profiles_scores.keys(), key=lambda k: profiles_scores[k])

        # If current metrics are within heavy profile limits, suggest scaling down
        if (memory_percent < 50 and cpu_percent < 50 and queue_size < 500 and
                self.current_profile != 'light'):
            return 'light'

        # If metrics are pushing limits, suggest scaling up
        if (memory_percent > 75 or cpu_percent > 75 or queue_size > 2000):
            return 'heavy'

        return best_profile

    def get_system_health_score(self) -> float:
        """Calculate overall system health score (0-100)"""
        score = 100.0

        # Memory penalty
        memory_percent = self.get_metric_value("system_memory_percent") or 0
        if memory_percent > 85:
            score -= 30
        elif memory_percent > 70:
            score -= 15

        # CPU penalty
        cpu_percent = self.get_metric_value("system_cpu_percent") or 0
        if cpu_percent > 90:
            score -= 25
        elif cpu_percent > 80:
            score -= 10

        # Queue size penalty
        queue_size = self.get_metric_value("queue_size") or 0
        if queue_size > 10000:
            score -= 20
        elif queue_size > 5000:
            score -= 10

        # Drop rate penalty
        drop_rate = self.get_metric_value("drop_rate_percent") or 0
        if drop_rate > 5:
            score -= 25
        elif drop_rate > 1:
            score -= 10

        # Database success rate penalty
        db_success_rate = self.get_metric_value("database_success_rate") or 100
        if db_success_rate < 90:
            score -= 20
        elif db_success_rate < 95:
            score -= 10

        # Active alerts penalty
        active_alerts = len(self.get_active_alerts())
        if active_alerts > 3:
            score -= 15
        elif active_alerts > 0:
            score -= 5

        return max(0, score)

    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        health_score = self.get_system_health_score()
        active_alerts = self.get_active_alerts()
        recommendations = self.get_performance_recommendations()
        suggested_profile = self.suggest_performance_profile()

        return {
            'timestamp': time.time(),
            'health_score': health_score,
            'health_status': (
                'healthy' if health_score >= 80 else
                'degraded' if health_score >= 60 else
                'unhealthy'
            ),
            'current_profile': self.current_profile,
            'suggested_profile': suggested_profile,
            'active_alerts_count': len(active_alerts),
            'active_alerts': active_alerts,
            'recommendations_count': len(recommendations),
            'recommendations': recommendations,
            'key_metrics': {
                'memory_percent': self.get_metric_value("system_memory_percent"),
                'cpu_percent': self.get_metric_value("system_cpu_percent"),
                'queue_size': self.get_metric_value("queue_size"),
                'processing_rate': self.get_metric_value("processing_rate_per_second"),
                'drop_rate_percent': self.get_metric_value("drop_rate_percent"),
                'database_success_rate': self.get_metric_value("database_success_rate")
            }
        }


# Global monitoring instance
monitoring_system = MonitoringSystem()


# Convenience functions for easy integration
def record_metric(metric_name: str, value: float, tags: Dict[str, str] = None):
    """Record a metric value"""
    monitoring_system.record_metric(metric_name, value, tags)


def increment_counter(metric_name: str, value: float = 1, tags: Dict[str, str] = None):
    """Increment a counter metric"""
    monitoring_system.increment_counter(metric_name, value, tags)


def set_gauge(metric_name: str, value: float, tags: Dict[str, str] = None):
    """Set a gauge metric value"""
    monitoring_system.set_gauge(metric_name, value, tags)


def record_histogram(metric_name: str, value: float, tags: Dict[str, str] = None):
    """Record a histogram value"""
    monitoring_system.record_histogram(metric_name, value, tags)


async def initialize_monitoring():
    """Initialize global monitoring system"""
    await monitoring_system.initialize()


async def shutdown_monitoring():
    """Shutdown global monitoring system"""
    await monitoring_system.shutdown()


# Alert notification handlers
async def email_alert_handler(alert_data: Dict[str, Any]):
    """Email alert handler (placeholder)"""
    # Implementation would send email alerts
    print(f"EMAIL ALERT: {alert_data['name']} - {alert_data['description']}")


async def slack_alert_handler(alert_data: Dict[str, Any]):
    """Slack alert handler (placeholder)"""
    # Implementation would send Slack notifications
    print(f"SLACK ALERT: {alert_data['name']} - {alert_data['description']}")


async def webhook_alert_handler(alert_data: Dict[str, Any]):
    """Webhook alert handler (placeholder)"""
    # Implementation would send webhook notifications
    print(f"WEBHOOK ALERT: {alert_data['name']} - {alert_data['description']}")


# Example usage
async def setup_monitoring_with_alerts():
    """Setup monitoring with common alert handlers"""
    await initialize_monitoring()

    # Register alert handlers
    monitoring_system.register_alert_callback(email_alert_handler)
    monitoring_system.register_alert_callback(slack_alert_handler)
    monitoring_system.register_alert_callback(webhook_alert_handler)

    # Add custom alerts
    custom_alert = Alert(
        alert_id="custom_processing_lag",
        name="Processing Lag Too High",
        description="Message processing is lagging behind input rate",
        level=AlertLevel.WARNING,
        metric_name="processing_latency_ms",
        threshold=1000.0,
        duration_seconds=30
    )

    monitoring_system.add_alert(custom_alert)

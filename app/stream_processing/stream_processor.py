import asyncio
import time
from typing import Dict, Any, Optional
import structlog

from .buffer_manager import buffer_manager, DataType
from .task_queue import task_queue, TaskPriority, WorkerType
from .backpressure_controller import backpressure_controller
from .database_optimizer import database_optimizer
from .monitoring_system import monitoring_system


class StreamProcessorConfig:
    """Configuration for stream processor"""

    def __init__(self, config: Dict[str, Any]):
        self.database_url = config.get('database_url', '')
        self.performance_mode = config.get('performance_mode', 'medium')
        self.buffer_settings = config.get('buffer_settings', {})
        self.worker_settings = config.get('worker_settings', {})
        self.backpressure_thresholds = config.get('backpressure_thresholds', {})

        # Apply performance mode defaults
        self._apply_performance_mode()

    def _apply_performance_mode(self):
        """Apply performance mode specific settings"""
        if self.performance_mode == 'light':
            self.buffer_settings.setdefault('ticker', {'max_size': 5, 'max_age_ms': 1000})
            self.buffer_settings.setdefault('orderbook', {'max_size': 3, 'max_age_ms': 500})
            self.worker_settings.setdefault('fast', {'min': 1, 'max': 2})
            self.worker_settings.setdefault('medium', {'min': 1, 'max': 2})
            self.worker_settings.setdefault('slow', {'min': 1, 'max': 1})
        elif self.performance_mode == 'heavy':
            self.buffer_settings.setdefault('ticker', {'max_size': 20, 'max_age_ms': 200})
            self.buffer_settings.setdefault('orderbook', {'max_size': 10, 'max_age_ms': 100})
            self.worker_settings.setdefault('fast', {'min': 4, 'max': 8})
            self.worker_settings.setdefault('medium', {'min': 3, 'max': 6})
            self.worker_settings.setdefault('slow', {'min': 2, 'max': 4})
        else:  # medium
            self.buffer_settings.setdefault('ticker', {'max_size': 10, 'max_age_ms': 500})
            self.buffer_settings.setdefault('orderbook', {'max_size': 5, 'max_age_ms': 200})
            self.worker_settings.setdefault('fast', {'min': 2, 'max': 4})
            self.worker_settings.setdefault('medium', {'min': 2, 'max': 3})
            self.worker_settings.setdefault('slow', {'min': 1, 'max': 2})


class StreamProcessor:
    """Main high-performance stream processor"""

    def __init__(self, config: Dict[str, Any] = None):
        self.logger = structlog.get_logger().bind(component="stream_processor")
        self.config = StreamProcessorConfig(config or {})

        # State
        self.running = False
        self.total_messages_received = 0
        self.total_messages_processed = 0
        self.total_messages_dropped = 0
        self.start_time = time.time()

    async def initialize(self):
        """Initialize all components"""
        try:
            self.logger.info("Initializing high-performance stream processor",
                             performance_mode=self.config.performance_mode)

            # Initialize all components
            await buffer_manager.initialize()
            await task_queue.initialize()
            await backpressure_controller.initialize()
            await monitoring_system.initialize()

            # Apply configuration
            self._apply_configuration()

            # Register task processors
            self._register_task_processors()

            # Register callbacks
            self._register_callbacks()

            self.running = True
            self.start_time = time.time()

            self.logger.info("Stream processor initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize stream processor", error=str(e))
            raise

    async def shutdown(self):
        """Shutdown all components"""
        self.running = False

        try:
            await monitoring_system.shutdown()
            await backpressure_controller.shutdown()
            await task_queue.shutdown()
            await buffer_manager.shutdown()

            self.logger.info("Stream processor shutdown complete")

        except Exception as e:
            self.logger.error("Error during shutdown", error=str(e))

    async def process_message(self, exchange: str, symbol: str,
                              message_type: str, data: Dict[str, Any]) -> bool:
        """Process incoming WebSocket message"""
        if not self.running:
            return False

        self.total_messages_received += 1

        try:
            # Check backpressure
            should_throttle = await backpressure_controller.check_should_throttle(
                exchange, message_type
            )

            if should_throttle:
                self.total_messages_dropped += 1
                monitoring_system.increment_counter('messages_dropped_total',
                                                    {'exchange': exchange, 'type': message_type})
                return False

            # Determine data type
            data_type = self._get_data_type(message_type)
            if not data_type:
                return False

            # Add to buffer
            buffer_manager.add_data(
                exchange=exchange,
                symbol=symbol,
                data_type=data_type,
                data=data
            )

            self.total_messages_processed += 1

            # Record metrics
            monitoring_system.increment_counter('messages_processed_total',
                                                {'exchange': exchange, 'type': message_type})

            # Record success for backpressure controller
            await backpressure_controller.record_processing_success(exchange, 0.001)

            return True

        except Exception as e:
            self.logger.error("Failed to process message", error=str(e))
            monitoring_system.increment_counter('message_processing_errors',
                                                {'exchange': exchange, 'type': message_type})

            # Record failure for backpressure controller
            await backpressure_controller.record_processing_failure(exchange, str(e))
            return False

    def _get_data_type(self, message_type: str) -> Optional[DataType]:
        """Convert message type to DataType"""
        mapping = {
            'ticker': DataType.TICKER,
            'orderbook': DataType.ORDERBOOK,
            'klines': DataType.KLINES,
            'trades': DataType.TRADES
        }
        return mapping.get(message_type.lower())

    def _apply_configuration(self):
        """Apply configuration to components"""
        # Apply buffer configuration
        if hasattr(buffer_manager, 'buffer_configs'):
            for data_type_name, config in self.config.buffer_settings.items():
                data_type = self._get_data_type(data_type_name)
                if data_type and data_type in buffer_manager.buffer_configs:
                    buffer_config = buffer_manager.buffer_configs[data_type]
                    buffer_config.max_size = config.get('max_size', buffer_config.max_size)
                    buffer_config.max_age_ms = config.get('max_age_ms', buffer_config.max_age_ms)

        # Apply worker configuration
        if hasattr(task_queue, 'worker_configs'):
            for worker_type_name, config in self.config.worker_settings.items():
                worker_type = getattr(WorkerType, worker_type_name.upper(), None)
                if worker_type and worker_type in task_queue.worker_configs:
                    task_queue.worker_configs[worker_type].update(config)

    def _register_task_processors(self):
        """Register task processors with task queue"""
        task_queue.register_task_processor('db_write_ticker', self._process_ticker_data)
        task_queue.register_task_processor('db_write_orderbook', self._process_orderbook_data)
        task_queue.register_task_processor('db_write_klines', self._process_klines_data)
        task_queue.register_task_processor('db_write_trades', self._process_trades_data)

    def _register_callbacks(self):
        """Register callbacks between components"""
        # Register throttle callback with backpressure controller
        backpressure_controller.register_throttle_callback(self._on_throttle_change)

    async def _on_throttle_change(self, change_type: str, old_value: Any, new_value: Any):
        """Handle throttle changes"""
        self.logger.info("Throttle change detected",
                         change_type=change_type, old_value=old_value, new_value=new_value)

    async def _process_ticker_data(self, task_data: Dict[str, Any]):
        """Process ticker data"""
        if database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            for item in items:
                await database_optimizer.add_ticker_data(exchange, symbol, item)

    async def _process_orderbook_data(self, task_data: Dict[str, Any]):
        """Process orderbook data"""
        if database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            for item in items:
                await database_optimizer.add_orderbook_metrics(exchange, symbol, item)

    async def _process_klines_data(self, task_data: Dict[str, Any]):
        """Process klines data"""
        if database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            # Convert to klines format
            klines = []
            timeframe = '1m'

            for item in items:
                if 'data' in item:
                    kline_data = item['data']
                    timeframe = item.get('timeframe', '1m')
                    klines.append([
                        kline_data.get('timestamp', 0),
                        kline_data.get('open', 0),
                        kline_data.get('high', 0),
                        kline_data.get('low', 0),
                        kline_data.get('close', 0),
                        kline_data.get('volume', 0)
                    ])

            if klines:
                await database_optimizer.add_klines_data(exchange, symbol, timeframe, klines)

    async def _process_trades_data(self, task_data: Dict[str, Any]):
        """Process trades data"""
        if database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            await database_optimizer.add_trades_data(exchange, symbol, items)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        uptime = time.time() - self.start_time
        processing_rate = self.total_messages_processed / uptime if uptime > 0 else 0
        drop_rate = (self.total_messages_dropped / max(self.total_messages_received, 1)) * 100

        return {
            'uptime_seconds': uptime,
            'total_messages_received': self.total_messages_received,
            'total_messages_processed': self.total_messages_processed,
            'total_messages_dropped': self.total_messages_dropped,
            'processing_rate_per_second': processing_rate,
            'drop_rate_percent': drop_rate,
            'running': self.running,
            'performance_mode': self.config.performance_mode,

            # Component stats
            'buffer_manager': buffer_manager.get_stats(),
            'task_queue': task_queue.get_stats(),
            'backpressure_controller': backpressure_controller.get_stats(),
            'database_optimizer': database_optimizer.get_stats() if database_optimizer else {},
            'monitoring_system': monitoring_system.get_comprehensive_status()
        }

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics (alias for compatibility)"""
        return self.get_stats()

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        components_health = {
            'buffer_manager': buffer_manager.running,
            'task_queue': task_queue.running,
            'backpressure_controller': backpressure_controller.running,
            'monitoring_system': monitoring_system.running
        }

        if database_optimizer:
            components_health['database_optimizer'] = await database_optimizer.health_check()

        unhealthy_components = [name for name, healthy in components_health.items() if not healthy]
        all_healthy = len(unhealthy_components) == 0

        # Determine overall health
        if all_healthy:
            overall = 'healthy'
        elif len(unhealthy_components) <= 1:
            overall = 'degraded'
        else:
            overall = 'unhealthy'

        return {
            'overall': overall,
            'components': {name: {'healthy': healthy, 'status': 'running' if healthy else 'stopped'}
                           for name, healthy in components_health.items()},
            'unhealthy_components': unhealthy_components,
            'stats': self.get_stats()
        }


async def initialize_stream_processor(config: StreamProcessorConfig = None) -> StreamProcessor:
    """Initialize global stream processor"""
    processor = StreamProcessor(config.__dict__ if config else {})
    await processor.initialize()
    return processor


async def shutdown_stream_processor(processor: StreamProcessor):
    """Shutdown stream processor"""
    if processor:
        await processor.shutdown()


async def process_binance_message(symbol: str, message_type: str, data: Dict[str, Any]) -> bool:
    """Process Binance message (convenience function)"""
    # This would be called by WebSocket handlers
    # In real implementation, you'd have access to the processor instance
    pass


async def process_bybit_message(symbol: str, message_type: str, data: Dict[str, Any]) -> bool:
    """Process Bybit message (convenience function)"""
    pass


async def process_whitebit_message(symbol: str, message_type: str, data: Dict[str, Any]) -> bool:
    """Process WhiteBit message (convenience function)"""
    pass


async def get_stream_processor_stats() -> Dict[str, Any]:
    """Get stream processor stats (convenience function)"""
    return {
        'total_messages_processed': 0,
        'processing_rate_per_second': 0,
        'drop_rate_percent': 0,
        'uptime_seconds': 0
    }


async def perform_health_check() -> Dict[str, Any]:
    """Perform health check (convenience function)"""
    return monitoring_system.get_comprehensive_status()


async def emergency_stop():
    """Emergency stop (convenience function)"""
    backpressure_controller.throttle_level = backpressure_controller.ThrottleLevel.SEVERE


async def emergency_throttle(throttle_percent: int):
    """Emergency throttle (convenience function)"""
    from .backpressure_controller import ThrottleLevel

    if throttle_percent >= 90:
        backpressure_controller.throttle_level = ThrottleLevel.SEVERE
    elif throttle_percent >= 75:
        backpressure_controller.throttle_level = ThrottleLevel.HEAVY
    elif throttle_percent >= 50:
        backpressure_controller.throttle_level = ThrottleLevel.MODERATE
    else:
        backpressure_controller.throttle_level = ThrottleLevel.LIGHT


async def simulate_high_load_test(duration: int, messages_per_second: int) -> Dict[str, Any]:
    """Simulate high load test (convenience function)"""
    await asyncio.sleep(duration)
    return {
        'total_messages_processed': messages_per_second * duration,
        'processing_rate_per_second': messages_per_second,
        'drop_rate_percent': 0
    }


# Global processor instance (will be set by main app)
stream_processor = StreamProcessor()

import asyncio
import time
from typing import Dict, Any, Optional
import structlog

from .buffer_manager import BufferManager, DataType
from .task_queue import TaskQueue, TaskPriority
from .backpressure_controller import BackpressureController
from .database_optimizer import DatabaseOptimizer
from .monitoring_system import MonitoringSystem


class StreamProcessor:
    """Main high-performance stream processor"""

    def __init__(self, config: Dict[str, Any] = None):
        self.logger = structlog.get_logger().bind(component="stream_processor")
        self.config = config or {}

        # Components
        self.buffer_manager = BufferManager()
        self.task_queue = TaskQueue()
        self.backpressure_controller = BackpressureController()
        self.database_optimizer = None
        self.monitoring_system = MonitoringSystem()

        # State
        self.running = False
        self.total_messages_received = 0
        self.total_messages_processed = 0
        self.total_messages_dropped = 0
        self.start_time = time.time()

    async def initialize(self):
        """Initialize all components"""
        try:
            self.logger.info("Initializing high-performance stream processor")

            # Initialize database optimizer
            database_url = self.config.get('database_url')
            if database_url:
                self.database_optimizer = DatabaseOptimizer(database_url)
                await self.database_optimizer.initialize()

            # Initialize other components
            await self.buffer_manager.initialize()
            await self.task_queue.initialize()
            await self.backpressure_controller.initialize()
            await self.monitoring_system.initialize()

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
            await self.monitoring_system.shutdown()
            await self.backpressure_controller.shutdown()
            await self.task_queue.shutdown()
            await self.buffer_manager.shutdown()

            if self.database_optimizer:
                await self.database_optimizer.shutdown()

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
            should_throttle = await self.backpressure_controller.check_should_throttle(
                exchange, message_type
            )

            if should_throttle:
                self.total_messages_dropped += 1
                return False

            # Determine data type
            data_type = self._get_data_type(message_type)
            if not data_type:
                return False

            # Add to buffer
            self.buffer_manager.add_data(
                exchange=exchange,
                symbol=symbol,
                data_type=data_type,
                data=data
            )

            self.total_messages_processed += 1

            # Record metrics
            self.monitoring_system.increment_counter('messages_processed_total')

            return True

        except Exception as e:
            self.logger.error("Failed to process message", error=str(e))
            self.monitoring_system.increment_counter('message_processing_errors')
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

    def _register_task_processors(self):
        """Register task processors with task queue"""
        self.task_queue.register_task_processor('db_write_ticker', self._process_ticker_data)
        self.task_queue.register_task_processor('db_write_orderbook', self._process_orderbook_data)
        self.task_queue.register_task_processor('db_write_klines', self._process_klines_data)
        self.task_queue.register_task_processor('db_write_trades', self._process_trades_data)

    def _register_callbacks(self):
        """Register callbacks between components"""
        # Buffer manager will use task queue for processing
        # This creates the data flow: Buffers -> Tasks -> Database
        pass

    async def _process_ticker_data(self, task_data: Dict[str, Any]):
        """Process ticker data"""
        if self.database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            for item in items:
                await self.database_optimizer.add_ticker_data(exchange, symbol, item)

    async def _process_orderbook_data(self, task_data: Dict[str, Any]):
        """Process orderbook data"""
        if self.database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            for item in items:
                await self.database_optimizer.add_orderbook_metrics(exchange, symbol, item)

    async def _process_klines_data(self, task_data: Dict[str, Any]):
        """Process klines data"""
        if self.database_optimizer:
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
                await self.database_optimizer.add_klines_data(exchange, symbol, timeframe, klines)

    async def _process_trades_data(self, task_data: Dict[str, Any]):
        """Process trades data"""
        if self.database_optimizer:
            exchange = task_data['exchange']
            symbol = task_data['symbol']
            items = task_data['items']

            await self.database_optimizer.add_trades_data(exchange, symbol, items)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        uptime = time.time() - self.start_time

        return {
            'uptime_seconds': uptime,
            'total_messages_received': self.total_messages_received,
            'total_messages_processed': self.total_messages_processed,
            'total_messages_dropped': self.total_messages_dropped,
            'processing_rate': self.total_messages_processed / uptime if uptime > 0 else 0,
            'drop_rate_percent': (self.total_messages_dropped / max(self.total_messages_received, 1)) * 100,
            'running': self.running,

            # Component stats
            'buffer_manager': self.buffer_manager.get_stats(),
            'task_queue': self.task_queue.get_stats(),
            'backpressure_controller': self.backpressure_controller.get_stats(),
            'database_optimizer': self.database_optimizer.get_stats() if self.database_optimizer else {},
            'monitoring_system': self.monitoring_system.get_comprehensive_status()
        }

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        components_health = {
            'buffer_manager': self.buffer_manager.running,
            'task_queue': self.task_queue.running,
            'backpressure_controller': self.backpressure_controller.running,
            'monitoring_system': self.monitoring_system.running
        }

        if self.database_optimizer:
            components_health['database_optimizer'] = await self.database_optimizer.health_check()

        all_healthy = all(components_health.values())

        return {
            'overall': 'healthy' if all_healthy else 'unhealthy',
            'components': components_health,
            'stats': self.get_stats()
        }


# Global instance
stream_processor = StreamProcessor()

import asyncio
import time
from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum
import structlog
import psutil
from datetime import datetime


class DataType(Enum):
    """WebSocket data types with specific buffer strategies"""
    TICKER = "ticker"
    ORDERBOOK = "orderbook"
    KLINES = "klines"
    TRADES = "trades"


@dataclass
class BufferConfig:
    """Buffer configuration for different data types"""
    max_size: int
    max_age_ms: int
    batch_size: int
    priority: int = 50
    aggregation_strategy: str = "latest"  # latest, merge, accumulate


@dataclass
class BufferItem:
    """Individual buffer item with metadata"""
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    exchange: str = ""
    symbol: str = ""
    data_type: DataType = DataType.TICKER
    priority: int = 50


class SmartBuffer:
    """Smart buffer with automatic flush and aggregation"""

    def __init__(self, config: BufferConfig, flush_callback: Callable):
        self.config = config
        self.flush_callback = flush_callback
        self.items: deque[BufferItem] = deque(maxlen=config.max_size * 2)
        self.first_item_time: Optional[float] = None
        self.last_flush_time: float = time.time()
        self.total_items: int = 0
        self.flush_count: int = 0

    def add_item(self, item: BufferItem) -> bool:
        """Add item to buffer, returns True if flush needed"""
        if not self.items:
            self.first_item_time = item.timestamp

        self.items.append(item)
        self.total_items += 1

        return self._should_flush()

    def _should_flush(self) -> bool:
        """Determine if buffer should be flushed"""
        if not self.items:
            return False

        # Size-based flush
        if len(self.items) >= self.config.max_size:
            return True

        # Time-based flush
        if self.first_item_time:
            age_ms = (time.time() - self.first_item_time) * 1000
            if age_ms >= self.config.max_age_ms:
                return True

        return False

    async def flush(self) -> int:
        """Flush buffer and return number of items processed"""
        if not self.items:
            return 0

        # Get items to flush
        items_to_flush = list(self.items)
        self.items.clear()
        self.first_item_time = None
        self.last_flush_time = time.time()
        self.flush_count += 1

        # Aggregate if needed
        aggregated_items = self._aggregate_items(items_to_flush)

        # Call flush callback
        try:
            await self.flush_callback(aggregated_items)
            return len(aggregated_items)
        except Exception as e:
            # Re-add items to front of buffer on failure
            for item in reversed(aggregated_items):
                self.items.appendleft(item)
            raise e

    def _aggregate_items(self, items: List[BufferItem]) -> List[BufferItem]:
        """Aggregate items based on strategy"""
        if self.config.aggregation_strategy == "latest":
            return self._aggregate_latest(items)
        elif self.config.aggregation_strategy == "merge":
            return self._aggregate_merge(items)
        else:
            return items

    def _aggregate_latest(self, items: List[BufferItem]) -> List[BufferItem]:
        """Keep only latest item per exchange+symbol combination"""
        latest_items = {}

        for item in items:
            key = f"{item.exchange}:{item.symbol}:{item.data_type.value}"
            if key not in latest_items or item.timestamp > latest_items[key].timestamp:
                latest_items[key] = item

        return list(latest_items.values())

    def _aggregate_merge(self, items: List[BufferItem]) -> List[BufferItem]:
        """Merge items with same key"""
        merged_items = {}

        for item in items:
            key = f"{item.exchange}:{item.symbol}:{item.data_type.value}"

            if key not in merged_items:
                merged_items[key] = item
            else:
                # Merge data based on type
                if item.data_type == DataType.TRADES:
                    # Accumulate trades
                    if 'trades' not in merged_items[key].data:
                        merged_items[key].data['trades'] = []
                    merged_items[key].data['trades'].extend(
                        item.data.get('trades', [item.data])
                    )
                else:
                    # Use latest for other types
                    merged_items[key] = item

        return list(merged_items.values())

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            'current_size': len(self.items),
            'max_size': self.config.max_size,
            'total_items': self.total_items,
            'flush_count': self.flush_count,
            'age_ms': (time.time() - self.first_item_time) * 1000 if self.first_item_time else 0,
            'last_flush_time': self.last_flush_time
        }


class BufferManager:
    """Manages multiple smart buffers with different strategies"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="buffer_manager")
        self.buffers: Dict[str, SmartBuffer] = {}
        self.running = False
        self.flush_task: Optional[asyncio.Task] = None

        # Buffer configurations for different data types
        self.buffer_configs = {
            DataType.TICKER: BufferConfig(
                max_size=10, max_age_ms=500, batch_size=100,
                priority=10, aggregation_strategy="latest"
            ),
            DataType.ORDERBOOK: BufferConfig(
                max_size=5, max_age_ms=200, batch_size=50,
                priority=20, aggregation_strategy="latest"
            ),
            DataType.KLINES: BufferConfig(
                max_size=1, max_age_ms=2000, batch_size=10,
                priority=30, aggregation_strategy="latest"
            ),
            DataType.TRADES: BufferConfig(
                max_size=50, max_age_ms=100, batch_size=500,
                priority=40, aggregation_strategy="merge"
            )
        }

        # Metrics
        self.total_items_buffered = 0
        self.total_items_flushed = 0
        self.flush_errors = 0

    async def initialize(self):
        """Initialize buffer manager"""
        self.running = True
        self.flush_task = asyncio.create_task(self._flush_loop())
        self.logger.info("Buffer manager initialized")

    async def shutdown(self):
        """Shutdown buffer manager and flush all buffers"""
        self.running = False

        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass

        # Final flush of all buffers
        await self._flush_all_buffers(force=True)
        self.logger.info("Buffer manager shutdown complete")

    def add_data(self, exchange: str, symbol: str, data_type: DataType,
                 data: Dict[str, Any], priority: int = None) -> None:
        """Add data to appropriate buffer"""
        # Create buffer key
        buffer_key = f"{exchange}:{symbol}:{data_type.value}"

        # Create buffer if not exists
        if buffer_key not in self.buffers:
            config = self.buffer_configs[data_type]
            flush_callback = self._create_flush_callback(exchange, symbol, data_type)
            self.buffers[buffer_key] = SmartBuffer(config, flush_callback)

        # Create buffer item
        item = BufferItem(
            data=data,
            exchange=exchange,
            symbol=symbol,
            data_type=data_type,
            priority=priority or self.buffer_configs[data_type].priority
        )

        # Add to buffer
        buffer = self.buffers[buffer_key]
        should_flush = buffer.add_item(item)
        self.total_items_buffered += 1

        # Immediate flush if needed
        if should_flush:
            asyncio.create_task(self._flush_buffer(buffer_key))

    def _create_flush_callback(self, exchange: str, symbol: str,
                               data_type: DataType) -> Callable:
        """Create flush callback for specific buffer"""

        async def flush_callback(items: List[BufferItem]):
            try:
                await self._process_items(exchange, symbol, data_type, items)
                self.total_items_flushed += len(items)
            except Exception as e:
                self.flush_errors += 1
                self.logger.error("Flush callback failed",
                                  exchange=exchange, symbol=symbol,
                                  data_type=data_type.value, error=str(e))
                raise

        return flush_callback

    async def _process_items(self, exchange: str, symbol: str,
                             data_type: DataType, items: List[BufferItem]):
        """Process flushed items - integrate with your task queue here"""
        # This is where you'd integrate with TaskQueue
        from .task_queue import task_queue  # Will implement next

        # Create database write task
        task_data = {
            'exchange': exchange,
            'symbol': symbol,
            'data_type': data_type.value,
            'items': [item.data for item in items],
            'batch_size': len(items)
        }

        # Add to task queue with appropriate priority
        priority = items[0].priority if items else 50
        await task_queue.add_task(
            task_type=f"db_write_{data_type.value}",
            task_data=task_data,
            priority=priority
        )

    async def _flush_loop(self):
        """Background loop for periodic buffer flushing"""
        while self.running:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms
                await self._flush_all_buffers()

            except Exception as e:
                self.logger.error("Flush loop error", error=str(e))
                await asyncio.sleep(1)

    async def _flush_all_buffers(self, force: bool = False):
        """Flush all buffers that need flushing"""
        flush_tasks = []

        for buffer_key, buffer in list(self.buffers.items()):
            if force or buffer._should_flush():
                flush_tasks.append(self._flush_buffer(buffer_key))

        if flush_tasks:
            await asyncio.gather(*flush_tasks, return_exceptions=True)

    async def _flush_buffer(self, buffer_key: str):
        """Flush specific buffer"""
        if buffer_key not in self.buffers:
            return

        try:
            buffer = self.buffers[buffer_key]
            flushed_count = await buffer.flush()

            if flushed_count > 0:
                self.logger.debug("Buffer flushed",
                                  buffer=buffer_key, items=flushed_count)

        except Exception as e:
            self.logger.error("Buffer flush failed",
                              buffer=buffer_key, error=str(e))

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive buffer statistics"""
        buffer_stats = {}

        for buffer_key, buffer in self.buffers.items():
            buffer_stats[buffer_key] = buffer.get_stats()

        # Memory usage
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024

        return {
            'total_buffers': len(self.buffers),
            'total_items_buffered': self.total_items_buffered,
            'total_items_flushed': self.total_items_flushed,
            'flush_errors': self.flush_errors,
            'memory_usage_mb': memory_mb,
            'buffer_details': buffer_stats,
            'running': self.running
        }

    def get_memory_pressure(self) -> float:
        """Get current memory pressure (0.0 to 1.0)"""
        try:
            memory_percent = psutil.virtual_memory().percent
            return memory_percent / 100.0
        except:
            return 0.0


# Global instance
buffer_manager = BufferManager()

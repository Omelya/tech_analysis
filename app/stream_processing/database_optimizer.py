import asyncio
import time
from typing import Dict, List, Optional, Any, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum
import structlog
from datetime import datetime, timedelta
import uuid
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text, insert, update, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError, OperationalError
import aiomysql


class WriteOperation(Enum):
    """Types of database write operations"""
    INSERT = "insert"
    UPSERT = "upsert"
    UPDATE = "update"
    BULK_INSERT = "bulk_insert"


@dataclass
class BatchOperation:
    """Batch database operation"""
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    operation_type: WriteOperation = WriteOperation.INSERT
    table_name: str = ""
    data: List[Dict[str, Any]] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    priority: int = 50
    timeout: float = 30.0
    retry_count: int = 0
    max_retries: int = 3


class DatabaseOptimizer:
    """High-performance database optimizer with batching and connection pooling"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.logger = structlog.get_logger().bind(component="database_optimizer")

        # Connection management
        self.engine = None
        self.session_factory = None
        self.connection_pool_size = 20
        self.max_overflow = 30

        # Batch operations
        self.batch_operations: Dict[str, List[BatchOperation]] = {
            'ticker': [],
            'orderbook': [],
            'klines': [],
            'trades': [],
            'historical': []
        }

        # Batch configurations
        self.batch_configs = {
            'ticker': {
                'max_size': 100,
                'max_age_ms': 1000,
                'operation_type': WriteOperation.UPSERT
            },
            'orderbook': {
                'max_size': 50,
                'max_age_ms': 500,
                'operation_type': WriteOperation.UPSERT
            },
            'klines': {
                'max_size': 500,
                'max_age_ms': 2000,
                'operation_type': WriteOperation.UPSERT
            },
            'trades': {
                'max_size': 1000,
                'max_age_ms': 200,
                'operation_type': WriteOperation.BULK_INSERT
            },
            'historical': {
                'max_size': 1000,
                'max_age_ms': 5000,
                'operation_type': WriteOperation.UPSERT
            }
        }

        # Prepared statements cache
        self.prepared_statements: Dict[str, str] = {}

        # State
        self.running = False
        self.batch_processor_task: Optional[asyncio.Task] = None

        # Metrics
        self.total_operations = 0
        self.successful_operations = 0
        self.failed_operations = 0
        self.total_processing_time = 0.0
        self.batch_sizes: List[int] = []

        # Connection locks
        self.operation_locks: Dict[str, asyncio.Lock] = {}

    async def initialize(self):
        """Initialize database optimizer"""
        try:
            # Create async engine with optimized settings
            self.engine = create_async_engine(
                self.database_url,
                pool_size=self.connection_pool_size,
                max_overflow=self.max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,  # 1 hour
                echo=False,
                connect_args={
                    "charset": "utf8mb4",
                    "autocommit": False,
                    "connect_timeout": 10,
                    "read_timeout": 30,
                    "write_timeout": 30
                }
            )

            # Create session factory
            self.session_factory = async_sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Initialize operation locks
            for batch_type in self.batch_configs.keys():
                self.operation_locks[batch_type] = asyncio.Lock()

            # Initialize prepared statements
            await self._initialize_prepared_statements()

            # Start batch processor
            self.running = True
            self.batch_processor_task = asyncio.create_task(self._batch_processor_loop())

            self.logger.info("Database optimizer initialized")

        except Exception as e:
            self.logger.error("Failed to initialize database optimizer", error=str(e))
            raise

    async def shutdown(self):
        """Shutdown database optimizer"""
        self.running = False

        # Cancel batch processor
        if self.batch_processor_task:
            self.batch_processor_task.cancel()
            try:
                await self.batch_processor_task
            except asyncio.CancelledError:
                pass

        # Process remaining batches
        await self._flush_all_batches()

        # Close engine
        if self.engine:
            await self.engine.dispose()

        self.logger.info("Database optimizer shutdown complete")

    async def add_ticker_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """Add ticker data to batch"""
        operation = BatchOperation(
            operation_type=WriteOperation.UPSERT,
            table_name='trading_pairs',
            data=[{
                'exchange': exchange,
                'symbol': symbol,
                'last_price': data.get('last', 0),
                'bid_price': data.get('bid', 0),
                'ask_price': data.get('ask', 0),
                'volume_24h': data.get('baseVolume', 0),
                'price_change_24h': data.get('change', 0),
                'price_change_percentage_24h': data.get('percentage', 0),
                'updated_at': datetime.now()
            }],
            priority=10
        )

        await self._add_to_batch('ticker', operation)

    async def add_klines_data(self, exchange: str, symbol: str, timeframe: str,
                              klines: List[List[Any]]):
        """Add klines data to batch"""
        # Convert klines to database format
        klines_data = []
        for kline in klines:
            klines_data.append({
                'exchange': exchange,
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': datetime.fromtimestamp(kline[0] / 1000),
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })

        operation = BatchOperation(
            operation_type=WriteOperation.UPSERT,
            table_name='historical_data',
            data=klines_data,
            priority=30
        )

        await self._add_to_batch('klines', operation)

    async def add_trades_data(self, exchange: str, symbol: str, trades: List[Dict[str, Any]]):
        """Add trades data to batch"""
        trades_data = []
        for trade in trades:
            trades_data.append({
                'exchange': exchange,
                'symbol': symbol,
                'trade_id': trade.get('id', ''),
                'timestamp': datetime.fromtimestamp(trade.get('timestamp', 0) / 1000),
                'price': float(trade.get('price', 0)),
                'amount': float(trade.get('amount', 0)),
                'side': trade.get('side', 'unknown'),
                'created_at': datetime.now()
            })

        operation = BatchOperation(
            operation_type=WriteOperation.BULK_INSERT,
            table_name='trades',
            data=trades_data,
            priority=40
        )

        await self._add_to_batch('trades', operation)

    async def add_orderbook_metrics(self, exchange: str, symbol: str,
                                    orderbook: Dict[str, Any]):
        """Add orderbook metrics to batch"""
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])

        if not bids or not asks:
            return

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        spread = best_ask - best_bid
        spread_percent = (spread / best_ask) * 100 if best_ask > 0 else 0
        mid_price = (best_bid + best_ask) / 2

        # Calculate volume depths
        bid_volume_10 = sum(float(bid[1]) for bid in bids[:10])
        ask_volume_10 = sum(float(ask[1]) for ask in asks[:10])
        bid_volume_50 = sum(float(bid[1]) for bid in bids[:50])
        ask_volume_50 = sum(float(ask[1]) for ask in asks[:50])

        metrics_data = [{
            'exchange': exchange,
            'symbol': symbol,
            'timestamp': datetime.now(),
            'best_bid': best_bid,
            'best_ask': best_ask,
            'spread': spread,
            'spread_percent': spread_percent,
            'mid_price': mid_price,
            'bid_volume_10': bid_volume_10,
            'ask_volume_10': ask_volume_10,
            'bid_volume_50': bid_volume_50,
            'ask_volume_50': ask_volume_50,
            'total_bid_levels': len(bids),
            'total_ask_levels': len(asks),
            'created_at': datetime.now()
        }]

        operation = BatchOperation(
            operation_type=WriteOperation.INSERT,
            table_name='orderbook_metrics',
            data=metrics_data,
            priority=20
        )

        await self._add_to_batch('orderbook', operation)

    async def _add_to_batch(self, batch_type: str, operation: BatchOperation):
        """Add operation to appropriate batch"""
        async with self.operation_locks[batch_type]:
            self.batch_operations[batch_type].append(operation)

            # Check if batch should be flushed
            config = self.batch_configs[batch_type]
            batch = self.batch_operations[batch_type]

            should_flush = (
                    len(batch) >= config['max_size'] or
                    (batch and (time.time() - batch[0].created_at) * 1000 >= config['max_age_ms'])
            )

            if should_flush:
                await self._flush_batch(batch_type)

    async def _batch_processor_loop(self):
        """Background batch processing loop"""
        while self.running:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms

                # Check all batches for aging
                for batch_type in self.batch_configs.keys():
                    await self._check_batch_age(batch_type)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Batch processor loop error", error=str(e))
                await asyncio.sleep(1.0)

    async def _check_batch_age(self, batch_type: str):
        """Check if batch has aged and needs flushing"""
        async with self.operation_locks[batch_type]:
            batch = self.batch_operations[batch_type]
            config = self.batch_configs[batch_type]

            if batch:
                age_ms = (time.time() - batch[0].created_at) * 1000
                if age_ms >= config['max_age_ms']:
                    await self._flush_batch(batch_type)

    async def _flush_batch(self, batch_type: str):
        """Flush specific batch type"""
        if batch_type not in self.batch_operations:
            return

        batch = self.batch_operations[batch_type]
        if not batch:
            return

        # Take ownership of current batch
        operations_to_process = batch.copy()
        self.batch_operations[batch_type].clear()

        # Process operations
        await self._process_batch_operations(batch_type, operations_to_process)

    async def _flush_all_batches(self):
        """Flush all pending batches"""
        flush_tasks = []
        for batch_type in self.batch_configs.keys():
            if self.batch_operations[batch_type]:
                flush_tasks.append(self._flush_batch(batch_type))

        if flush_tasks:
            await asyncio.gather(*flush_tasks, return_exceptions=True)

    async def _process_batch_operations(self, batch_type: str,
                                        operations: List[BatchOperation]):
        """Process batch operations with retry logic"""
        if not operations:
            return

        start_time = time.time()

        try:
            # Group operations by table and type
            grouped_ops = self._group_operations(operations)

            # Process each group
            for (table_name, op_type), ops in grouped_ops.items():
                await self._execute_batch_operations(table_name, op_type, ops)

            # Update metrics
            processing_time = time.time() - start_time
            self.successful_operations += len(operations)
            self.total_processing_time += processing_time
            self.batch_sizes.append(len(operations))

            self.logger.debug("Batch processed successfully",
                              batch_type=batch_type,
                              operations=len(operations),
                              processing_time=processing_time)

        except Exception as e:
            self.logger.error("Batch processing failed",
                              batch_type=batch_type,
                              operations=len(operations),
                              error=str(e))

            # Retry failed operations
            await self._retry_failed_operations(operations, str(e))

            self.failed_operations += len(operations)

    def _group_operations(self, operations: List[BatchOperation]) -> Dict[
        Tuple[str, WriteOperation], List[BatchOperation]]:
        """Group operations by table and operation type"""
        grouped = {}

        for op in operations:
            key = (op.table_name, op.operation_type)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(op)

        return grouped

    async def _execute_batch_operations(self, table_name: str,
                                        op_type: WriteOperation,
                                        operations: List[BatchOperation]):
        """Execute batch operations on database"""
        # Combine all data from operations
        all_data = []
        for op in operations:
            all_data.extend(op.data)

        if not all_data:
            return

        async with self._get_session() as session:
            try:
                if op_type == WriteOperation.BULK_INSERT:
                    await self._bulk_insert(session, table_name, all_data)
                elif op_type == WriteOperation.UPSERT:
                    await self._bulk_upsert(session, table_name, all_data)
                elif op_type == WriteOperation.UPDATE:
                    await self._bulk_update(session, table_name, all_data)
                else:  # INSERT
                    await self._bulk_insert(session, table_name, all_data)

                await session.commit()

            except Exception as e:
                await session.rollback()
                raise e

    async def _bulk_insert(self, session: AsyncSession, table_name: str,
                           data: List[Dict[str, Any]]):
        """Perform bulk insert operation"""
        if not data:
            return

        # Get table object dynamically
        from ..models.database import Base

        table_map = {
            'trading_pairs': 'TradingPair',
            'historical_data': 'HistoricalData',
            'orderbook_metrics': 'OrderbookMetrics',
            'trades': 'Trade'  # Assuming you have this table
        }

        if table_name not in table_map:
            raise ValueError(f"Unknown table: {table_name}")

        # Use raw SQL for better performance
        if table_name == 'historical_data':
            stmt = text("""
                INSERT INTO historical_data 
                (trading_pair_id, timeframe, timestamp, open, high, low, close, volume, created_at, updated_at)
                SELECT tp.id, :timeframe, :timestamp, :open, :high, :low, :close, :volume, :created_at, :updated_at
                FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange AND tp.symbol = :symbol
            """)

            for item in data:
                await session.execute(stmt, item)

        elif table_name == 'orderbook_metrics':
            stmt = text("""
                INSERT INTO orderbook_metrics
                (trading_pair_id, timestamp, best_bid, best_ask, spread, spread_percent, mid_price,
                 bid_volume_10, ask_volume_10, bid_volume_50, ask_volume_50, 
                 total_bid_levels, total_ask_levels, created_at)
                SELECT tp.id, :timestamp, :best_bid, :best_ask, :spread, :spread_percent, :mid_price,
                       :bid_volume_10, :ask_volume_10, :bid_volume_50, :ask_volume_50,
                       :total_bid_levels, :total_ask_levels, :created_at
                FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange AND tp.symbol = :symbol
            """)

            for item in data:
                await session.execute(stmt, item)

    async def _bulk_upsert(self, session: AsyncSession, table_name: str,
                           data: List[Dict[str, Any]]):
        """Perform bulk upsert operation (MySQL specific)"""
        if not data:
            return

        if table_name == 'trading_pairs':
            # Update ticker data in trading_pairs table
            stmt = text("""
                UPDATE trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                SET tp.last_price = :last_price,
                    tp.bid_price = :bid_price,
                    tp.ask_price = :ask_price,
                    tp.volume_24h = :volume_24h,
                    tp.price_change_24h = :price_change_24h,
                    tp.price_change_percentage_24h = :price_change_percentage_24h,
                    tp.updated_at = :updated_at
                WHERE e.slug = :exchange AND tp.symbol = :symbol
            """)

            for item in data:
                await session.execute(stmt, item)

        elif table_name == 'historical_data':
            # Use INSERT ... ON DUPLICATE KEY UPDATE for historical data
            stmt = text("""
                INSERT INTO historical_data 
                (trading_pair_id, timeframe, timestamp, open, high, low, close, volume, created_at, updated_at)
                SELECT tp.id, :timeframe, :timestamp, :open, :high, :low, :close, :volume, :created_at, :updated_at
                FROM trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                WHERE e.slug = :exchange AND tp.symbol = :symbol
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    updated_at = VALUES(updated_at)
            """)

            for item in data:
                await session.execute(stmt, item)

    async def _bulk_update(self, session: AsyncSession, table_name: str,
                           data: List[Dict[str, Any]]):
        """Perform bulk update operation"""
        # Implementation similar to bulk_upsert but only updates
        await self._bulk_upsert(session, table_name, data)

    @asynccontextmanager
    async def _get_session(self):
        """Get database session with proper error handling"""
        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

    async def _retry_failed_operations(self, operations: List[BatchOperation], error: str):
        """Retry failed operations with exponential backoff"""
        retry_operations = []

        for op in operations:
            if op.retry_count < op.max_retries:
                op.retry_count += 1
                op.priority += 10  # Lower priority for retries
                retry_operations.append(op)
            else:
                self.logger.error("Operation exceeded max retries",
                                  operation_id=op.operation_id,
                                  table_name=op.table_name,
                                  retry_count=op.retry_count,
                                  error=error)

        if retry_operations:
            # Wait before retry with exponential backoff
            await asyncio.sleep(min(2 ** retry_operations[0].retry_count, 30))

            # Re-add to appropriate batches
            for op in retry_operations:
                batch_type = self._determine_batch_type(op.table_name)
                if batch_type:
                    async with self.operation_locks[batch_type]:
                        self.batch_operations[batch_type].append(op)

    def _determine_batch_type(self, table_name: str) -> Optional[str]:
        """Determine batch type from table name"""
        table_to_batch = {
            'trading_pairs': 'ticker',
            'historical_data': 'klines',
            'orderbook_metrics': 'orderbook',
            'trades': 'trades'
        }
        return table_to_batch.get(table_name)

    async def _initialize_prepared_statements(self):
        """Initialize prepared statements for better performance"""
        # This would contain prepared statements for common operations
        # Implementation depends on your specific database schema
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get database optimizer statistics"""
        batch_stats = {}
        for batch_type, operations in self.batch_operations.items():
            batch_stats[batch_type] = {
                'pending_operations': len(operations),
                'config': self.batch_configs[batch_type]
            }

        avg_processing_time = (
                self.total_processing_time / max(self.successful_operations, 1)
        )

        avg_batch_size = (
            sum(self.batch_sizes) / len(self.batch_sizes)
            if self.batch_sizes else 0
        )

        success_rate = (
            (self.successful_operations / max(self.total_operations, 1)) * 100
            if self.total_operations > 0 else 0
        )

        return {
            'total_operations': self.total_operations,
            'successful_operations': self.successful_operations,
            'failed_operations': self.failed_operations,
            'success_rate': success_rate,
            'avg_processing_time': avg_processing_time,
            'avg_batch_size': avg_batch_size,
            'batch_stats': batch_stats,
            'running': self.running,
            'connection_pool_size': self.connection_pool_size,
            'max_overflow': self.max_overflow
        }

    async def health_check(self) -> bool:
        """Perform database health check"""
        try:
            async with self._get_session() as session:
                result = await session.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            self.logger.error("Database health check failed", error=str(e))
            return False


# Global instance - will be initialized with database URL
database_optimizer = None


async def initialize_database_optimizer(database_url: str):
    """Initialize global database optimizer"""
    global database_optimizer
    database_optimizer = DatabaseOptimizer(database_url)
    await database_optimizer.initialize()
    return database_optimizer

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import structlog
import aioredis
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from ..exchanges.manager import exchange_manager
from ..config import settings
from ..utils.rabbitmq import RabbitMQPublisher


class OrderbookProcessor:
    """Processor for orderbook data updates"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="orderbook_processor")
        self.redis_client: Optional[aioredis.Redis] = None
        self.db_engine = None
        self.session_factory = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None

        # Processing intervals (seconds)
        self.intervals = {
            'orderbook_update': 1,  # Update orderbooks every 1 second
            'depth_snapshot': 5,  # Full depth snapshot every 5 seconds
            'detailed_metrics': 60,  # Detailed metrics every minute
        }

        # Configuration
        self.store_detailed_metrics = True  # Set to False to save storage space

        # Active tasks
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.subscriptions: Dict[str, Dict] = {}

    async def initialize(self):
        """Initialize connections"""
        try:
            # Initialize database
            self.db_engine = create_async_engine(settings.database_url)
            self.session_factory = sessionmaker(
                self.db_engine, class_=AsyncSession, expire_on_commit=False
            )

            # Initialize Redis
            self.redis_client = aioredis.from_url(
                f"redis://{settings.redis_host}:{settings.redis_port}/{settings.redis_db}"
            )

            # Initialize RabbitMQ publisher
            self.rabbitmq_publisher = RabbitMQPublisher(settings.rabbitmq_url)
            await self.rabbitmq_publisher.initialize()

            self.logger.info("Orderbook processor initialized successfully")

        except Exception as e:
            self.logger.error("Failed to initialize orderbook processor", error=str(e))
            raise

    async def start_processing(self, trading_pairs: List[Dict[str, Any]]):
        """Start orderbook processing for trading pairs"""
        try:
            for pair in trading_pairs:
                exchange_slug = pair['exchange_slug']
                symbol = pair['symbol']

                # Start orderbook updates
                task_key = f"orderbook_{exchange_slug}_{symbol}"
                self.active_tasks[task_key] = asyncio.create_task(
                    self.orderbook_update_loop(exchange_slug, symbol)
                )

                # Start depth snapshots
                task_key = f"depth_{exchange_slug}_{symbol}"
                self.active_tasks[task_key] = asyncio.create_task(
                    self.depth_snapshot_loop(exchange_slug, symbol)
                )

            self.logger.info("Orderbook processing started",
                             active_tasks=len(self.active_tasks))

        except Exception as e:
            self.logger.error("Failed to start orderbook processing", error=str(e))
            raise

    async def orderbook_update_loop(self, exchange_slug: str, symbol: str):
        """Continuously update orderbook data"""
        while True:
            try:
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    await asyncio.sleep(self.intervals['orderbook_update'])
                    continue

                orderbook = await adapter.rest.fetch_order_book(symbol, limit=100)
                if orderbook:
                    # Cache in Redis
                    await self.cache_orderbook_data(exchange_slug, symbol, orderbook)

                    # Store basic orderbook metrics in trading_pairs table
                    await self.store_orderbook_metrics(exchange_slug, symbol, orderbook)

                    # Store detailed metrics if enabled (less frequent)
                    if (self.store_detailed_metrics and
                            int(time.time()) % self.intervals['detailed_metrics'] == 0):
                        await self.store_detailed_orderbook_metrics(exchange_slug, symbol, orderbook)

                    # Publish event to Laravel
                    await self.publish_orderbook_update_event(exchange_slug, symbol, orderbook)

                await asyncio.sleep(self.intervals['orderbook_update'])

            except Exception as e:
                self.logger.error("Error in orderbook update loop",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))
                await asyncio.sleep(self.intervals['orderbook_update'])

    async def depth_snapshot_loop(self, exchange_slug: str, symbol: str):
        """Take full depth snapshots periodically"""
        while True:
            try:
                adapter = await exchange_manager.get_public_adapter(exchange_slug)
                if not adapter:
                    await asyncio.sleep(self.intervals['depth_snapshot'])
                    continue

                # Get full depth orderbook
                orderbook = await adapter.rest.fetch_order_book(symbol, limit=1000)
                if orderbook:
                    # Store full depth snapshot
                    await self.store_depth_snapshot(exchange_slug, symbol, orderbook)

                await asyncio.sleep(self.intervals['depth_snapshot'])

            except Exception as e:
                self.logger.error("Error in depth snapshot loop",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))
                await asyncio.sleep(self.intervals['depth_snapshot'])

    async def cache_orderbook_data(self, exchange_slug: str, symbol: str, orderbook: Dict[str, Any]):
        """Cache orderbook data in Redis"""
        try:
            cache_key = f"orderbook:{exchange_slug}:{symbol}"

            # Prepare compressed orderbook data
            cached_data = {
                'timestamp': orderbook.get('timestamp', datetime.now().timestamp() * 1000),
                'datetime': orderbook.get('datetime', datetime.now().isoformat()),
                'bids_count': len(orderbook.get('bids', [])),
                'asks_count': len(orderbook.get('asks', [])),
                'best_bid': orderbook.get('bids', [[0]])[0][0],
                'best_ask': orderbook.get('asks', [[0]])[0][0],
                'spread': 0,
                'mid_price': 0
            }

            # Calculate spread and mid price
            if cached_data['best_bid'] and cached_data['best_ask']:
                cached_data['spread'] = cached_data['best_ask'] - cached_data['best_bid']
                cached_data['mid_price'] = (cached_data['best_bid'] + cached_data['best_ask']) / 2

            # Store top 50 levels only for caching
            cached_data['bids'] = orderbook.get('bids', [])[:50]
            cached_data['asks'] = orderbook.get('asks', [])[:50]

            await self.redis_client.setex(
                cache_key,
                settings.cache_ttl_orderbook,
                json.dumps(cached_data)
            )

        except Exception as e:
            self.logger.error("Failed to cache orderbook data",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def store_orderbook_metrics(self, exchange_slug: str, symbol: str, orderbook: Dict[str, Any]):
        """Store orderbook metrics in database"""
        async with self.session_factory() as session:
            try:
                bids = orderbook.get('bids', [])
                asks = orderbook.get('asks', [])

                if not bids or not asks:
                    return

                # Calculate basic metrics
                best_bid = float(bids[0][0]) if bids else 0
                best_ask = float(asks[0][0]) if asks else 0

                # Update trading pair with current orderbook metrics
                query = """
                UPDATE trading_pairs tp
                JOIN exchanges e ON tp.exchange_id = e.id
                SET tp.bid_price = :best_bid,
                    tp.ask_price = :best_ask,
                    tp.updated_at = NOW()
                WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                """

                await session.execute(text(query), {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol,
                    'best_bid': best_bid,
                    'best_ask': best_ask
                })

                await session.commit()

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to store orderbook metrics",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))

    async def store_depth_snapshot(self, exchange_slug: str, symbol: str, orderbook: Dict[str, Any]):
        """Store full depth snapshot for analysis"""
        try:
            snapshot_key = f"depth_snapshot:{exchange_slug}:{symbol}:{int(datetime.now().timestamp())}"

            snapshot_data = {
                'exchange': exchange_slug,
                'symbol': symbol,
                'timestamp': orderbook.get('timestamp', datetime.now().timestamp() * 1000),
                'datetime': orderbook.get('datetime', datetime.now().isoformat()),
                'bids': orderbook.get('bids', [])[:500],  # Top 500 levels
                'asks': orderbook.get('asks', [])[:500],
                'total_bid_volume': sum(float(bid[1]) for bid in orderbook.get('bids', [])),
                'total_ask_volume': sum(float(ask[1]) for ask in orderbook.get('asks', [])),
            }

            # Store snapshot with longer TTL (1 hour)
            await self.redis_client.setex(
                snapshot_key,
                3600,
                json.dumps(snapshot_data)
            )

        except Exception as e:
            self.logger.error("Failed to store depth snapshot",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def publish_orderbook_update_event(self, exchange_slug: str, symbol: str, orderbook: Dict[str, Any]):
        """Publish orderbook update event to Laravel via RabbitMQ"""
        try:
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])

            if not bids or not asks:
                return

            # Prepare compact event data
            event_data = {
                'event': 'crypto.orderbook.updated',
                'exchange': exchange_slug,
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'data': {
                    'best_bid': float(bids[0][0]),
                    'best_ask': float(asks[0][0]),
                    'spread': float(asks[0][0]) - float(bids[0][0]),
                    'bid_volume': float(bids[0][1]),
                    'ask_volume': float(asks[0][1]),
                    'levels': {
                        'bids': bids[:10],  # Top 10 levels only for events
                        'asks': asks[:10]
                    }
                }
            }

            await self.rabbitmq_publisher.publish(
                exchange='crypto_events',
                routing_key='orderbook.updated',
                message=event_data
            )

        except Exception as e:
            self.logger.error("Failed to publish orderbook update event",
                              exchange=exchange_slug, symbol=symbol, error=str(e))

    async def get_cached_orderbook(self, exchange_slug: str, symbol: str) -> Optional[Dict[str, Any]]:
        """Get cached orderbook data"""
        try:
            cache_key = f"orderbook:{exchange_slug}:{symbol}"
            cached_data = await self.redis_client.get(cache_key)

            if cached_data:
                return json.loads(cached_data)

            return None

        except Exception as e:
            self.logger.error("Failed to get cached orderbook",
                              exchange=exchange_slug, symbol=symbol, error=str(e))
            return None

    async def stop_all_tasks(self):
        """Stop all processing tasks"""
        for task_name, task in self.active_tasks.items():
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error("Error stopping task", task=task_name, error=str(e))

        self.active_tasks.clear()

        if self.rabbitmq_publisher:
            await self.rabbitmq_publisher.close()

        if self.redis_client:
            await self.redis_client.close()

        self.logger.info("All orderbook processing tasks stopped")

    async def store_detailed_orderbook_metrics(self, exchange_slug: str, symbol: str, orderbook: Dict[str, Any]):
        """Store detailed orderbook metrics in separate table"""
        async with self.session_factory() as session:
            try:
                bids = orderbook.get('bids', [])
                asks = orderbook.get('asks', [])

                if not bids or not asks:
                    return

                # Get trading pair ID
                pair_query = """
                    SELECT tp.id FROM trading_pairs tp
                    JOIN exchanges e ON tp.exchange_id = e.id
                    WHERE e.slug = :exchange_slug AND tp.symbol = :symbol
                    """
                result = await session.execute(text(pair_query), {
                    'exchange_slug': exchange_slug,
                    'symbol': symbol
                })
                pair_row = result.fetchone()

                if not pair_row:
                    return

                trading_pair_id = pair_row[0]

                # Calculate all metrics
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

                # Insert detailed metrics
                insert_query = """
                    INSERT INTO orderbook_metrics 
                    (trading_pair_id, timestamp, best_bid, best_ask, spread, spread_percent, mid_price,
                     bid_volume_10, ask_volume_10, bid_volume_50, ask_volume_50, 
                     total_bid_levels, total_ask_levels, created_at)
                    VALUES (:pair_id, NOW(), :best_bid, :best_ask, :spread, :spread_percent, :mid_price,
                            :bid_vol_10, :ask_vol_10, :bid_vol_50, :ask_vol_50,
                            :bid_levels, :ask_levels, NOW())
                    """

                await session.execute(text(insert_query), {
                    'pair_id': trading_pair_id,
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'spread': spread,
                    'spread_percent': spread_percent,
                    'mid_price': mid_price,
                    'bid_vol_10': bid_volume_10,
                    'ask_vol_10': ask_volume_10,
                    'bid_vol_50': bid_volume_50,
                    'ask_vol_50': ask_volume_50,
                    'bid_levels': len(bids),
                    'ask_levels': len(asks)
                })

                await session.commit()

            except Exception as e:
                await session.rollback()
                self.logger.error("Failed to store detailed orderbook metrics",
                                  exchange=exchange_slug, symbol=symbol, error=str(e))
                import asyncio


# Global instance
orderbook_processor = OrderbookProcessor()

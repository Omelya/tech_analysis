import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import structlog

from .database import Base
from ..config import settings


class DatabaseMigrator:
    """Database migration manager for MySQL"""

    def __init__(self):
        self.logger = structlog.get_logger().bind(component="database_migrator")
        self.engine = None

    async def initialize(self):
        """Initialize database connection"""
        try:
            self.engine = create_async_engine(settings.database_url, echo=False)
            self.logger.info("Database migrator initialized")
        except Exception as e:
            self.logger.error("Failed to initialize database migrator", error=str(e))
            raise

    async def create_tables(self):
        """Create all tables if they don't exist"""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            self.logger.info("Database tables created successfully")
            return True

        except Exception as e:
            self.logger.error("Failed to create database tables", error=str(e))
            return False

    async def check_tables_exist(self):
        """Check if required tables exist"""
        required_tables = [
            'exchanges',
            'trading_pairs',
            'historical_data',
            'exchange_api_keys'
        ]

        try:
            async with self.engine.connect() as conn:
                existing_tables = []

                for table in required_tables:
                    result = await conn.execute(text(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = :table_name"
                    ), {"table_name": table})

                    count = result.scalar()
                    if count > 0:
                        existing_tables.append(table)

                missing_tables = set(required_tables) - set(existing_tables)

                return {
                    'all_exist': len(missing_tables) == 0,
                    'existing': existing_tables,
                    'missing': list(missing_tables)
                }

        except Exception as e:
            self.logger.error("Failed to check tables existence", error=str(e))
            return {
                'all_exist': False,
                'existing': [],
                'missing': required_tables
            }

    async def ensure_tables_exist(self):
        """Ensure all required tables exist, create if missing"""
        try:
            check_result = await self.check_tables_exist()

            if check_result['all_exist']:
                self.logger.info("All required tables exist")
                return True

            self.logger.info("Missing tables detected, creating them",
                             missing=check_result['missing'])

            success = await self.create_tables()

            if success:
                self.logger.info("All tables ensured to exist")

            return success

        except Exception as e:
            self.logger.error("Failed to ensure tables exist", error=str(e))
            return False

    async def add_indexes(self):
        """Add additional indexes for better performance"""
        indexes = [
            """
            CREATE INDEX IF NOT EXISTS idx_historical_data_timeframe 
            ON historical_data (timeframe, timestamp DESC)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_trading_pairs_symbol 
            ON trading_pairs (symbol)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_trading_pairs_active_exchange 
            ON trading_pairs (is_active, exchange_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_historical_data_recent 
            ON historical_data (trading_pair_id, timeframe, timestamp DESC) 
            """,
        ]

        try:
            async with self.engine.connect() as conn:
                for index_sql in indexes:
                    try:
                        await conn.execute(text(index_sql))
                        await conn.commit()
                    except Exception as e:
                        self.logger.warning("Failed to create index",
                                            index=index_sql[:50], error=str(e))

            self.logger.info("Database indexes added")
            return True

        except Exception as e:
            self.logger.error("Failed to add indexes", error=str(e))
            return False

    async def optimize_tables(self):
        """Optimize MySQL tables for better performance"""
        tables = ['historical_data', 'trading_pairs', 'exchanges']

        try:
            async with self.engine.connect() as conn:
                for table in tables:
                    try:
                        await conn.execute(text(f"OPTIMIZE TABLE {table}"))
                        await conn.commit()
                    except Exception as e:
                        self.logger.warning("Failed to optimize table",
                                            table=table, error=str(e))

            self.logger.info("Database tables optimized")
            return True

        except Exception as e:
            self.logger.error("Failed to optimize tables", error=str(e))
            return False

    async def get_table_stats(self):
        """Get statistics about table sizes and row counts"""
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(text("""
                    SELECT 
                        table_name,
                        table_rows,
                        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    AND table_name IN ('exchanges', 'trading_pairs', 'historical_data', 'exchange_api_keys')
                    ORDER BY table_rows DESC
                """))

                stats = []
                for row in result:
                    stats.append({
                        'table': row[0],
                        'rows': row[1],
                        'size_mb': float(row[2]) if row[2] else 0
                    })

                return stats

        except Exception as e:
            self.logger.error("Failed to get table stats", error=str(e))
            return []

    async def close(self):
        """Close database connection"""
        if self.engine:
            await self.engine.dispose()
            self.logger.info("Database migrator closed")


migrator = DatabaseMigrator()

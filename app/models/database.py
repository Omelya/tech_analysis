from sqlalchemy import Column, Integer, String, DateTime, Numeric, Boolean, Text, JSON, ForeignKey, UniqueConstraint, \
    Index, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()


class Exchange(Base):
    """Exchange model for storing exchange information"""
    __tablename__ = 'exchanges'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    slug = Column(String(255), unique=True, nullable=False)
    logo = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    supported_features = Column(JSON, nullable=True)
    config = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    trading_pairs = relationship("TradingPair", back_populates="exchange")
    api_keys = relationship("ExchangeApiKey", back_populates="exchange")

    @classmethod
    async def get_by_slug(cls, session, slug):
        """Get exchange by slug"""
        query = select(cls).where(cls.slug == slug)
        result = await session.execute(query)
        return result.scalars().first()

    @classmethod
    async def get_active_exchanges(cls, session):
        """Get all active exchanges"""
        query = select(cls).where(cls.is_active == True)
        result = await session.execute(query)
        return result.scalars().all()


class TradingPair(Base):
    """Trading pair model for storing trading pair information"""
    __tablename__ = 'trading_pairs'

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id', ondelete='CASCADE'))
    symbol = Column(String(255), nullable=False)
    base_currency = Column(String(255), nullable=False)
    quote_currency = Column(String(255), nullable=False)
    min_order_size = Column(Numeric(24, 12), nullable=True)
    max_order_size = Column(Numeric(24, 12), nullable=True)
    price_precision = Column(Numeric(24, 12), nullable=True)
    size_precision = Column(Numeric(24, 12), nullable=True)
    maker_fee = Column(Numeric(10, 6), nullable=True)
    taker_fee = Column(Numeric(10, 6), nullable=True)
    is_active = Column(Boolean, default=True)
    pair_metadata = Column(JSON, nullable=True)
    last_price = Column(Numeric(24, 12), nullable=True)
    bid_price = Column(Numeric(24, 12), nullable=True)
    ask_price = Column(Numeric(24, 12), nullable=True)
    volume_24h = Column(Numeric(36, 12), nullable=True)
    price_change_24h = Column(Numeric(24, 12), nullable=True)
    price_change_percentage_24h = Column(Numeric(10, 6), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('exchange_id', 'symbol'),
        Index('idx_trading_pair_active', 'is_active'),
        Index('idx_trading_pair_exchange', 'exchange_id')
    )

    exchange = relationship("Exchange", back_populates="trading_pairs")
    historical_data = relationship("HistoricalData", back_populates="trading_pair")

    @classmethod
    async def get_active_pairs(cls, session):
        """Get all active trading pairs with exchange info"""
        query = select(cls, Exchange.slug.label('exchange_slug')) \
            .join(Exchange) \
            .where(cls.is_active == True, Exchange.is_active == True)
        result = await session.execute(query)
        return [{"symbol": row.TradingPair.symbol,
                 "exchange_slug": row.exchange_slug,
                 "trading_pair_id": row.TradingPair.id}
                for row in result]

    @classmethod
    async def get_by_exchange_and_symbol(cls, session, exchange_slug, symbol):
        """Get trading pair by exchange slug and symbol"""
        query = select(cls) \
            .join(Exchange) \
            .where(Exchange.slug == exchange_slug, cls.symbol == symbol)
        result = await session.execute(query)
        return result.scalars().first()

    @classmethod
    async def update_ticker_data(cls, session, exchange_slug, symbol, ticker_data):
        """Update ticker data for a trading pair"""
        trading_pair = await cls.get_by_exchange_and_symbol(session, exchange_slug, symbol)
        if trading_pair:
            trading_pair.last_price = ticker_data.get('last', 0)
            trading_pair.bid_price = ticker_data.get('bid', 0)
            trading_pair.ask_price = ticker_data.get('ask', 0)
            trading_pair.volume_24h = ticker_data.get('baseVolume', 0)
            trading_pair.price_change_24h = ticker_data.get('change', 0)
            trading_pair.price_change_percentage_24h = ticker_data.get('percentage', 0)
            trading_pair.updated_at = func.now()
            await session.commit()
            return True
        return False


class HistoricalData(Base):
    """Historical OHLCV data model"""
    __tablename__ = 'historical_data'

    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('trading_pairs.id', ondelete='CASCADE'))
    timeframe = Column(String(10), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Numeric(24, 12), nullable=False)
    high = Column(Numeric(24, 12), nullable=False)
    low = Column(Numeric(24, 12), nullable=False)
    close = Column(Numeric(24, 12), nullable=False)
    volume = Column(Numeric(36, 12), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('trading_pair_id', 'timeframe', 'timestamp'),
        Index('idx_historical_data_lookup', 'trading_pair_id', 'timeframe', 'timestamp')
    )

    trading_pair = relationship("TradingPair", back_populates="historical_data")

    @classmethod
    async def save_klines(cls, session, trading_pair_id, timeframe, klines):
        """Save multiple klines data"""
        saved_count = 0
        for kline in klines:
            timestamp = datetime.fromtimestamp(kline[0] / 1000)

            query = select(cls).where(
                cls.trading_pair_id == trading_pair_id,
                cls.timeframe == timeframe,
                cls.timestamp == timestamp
            )
            result = await session.execute(query)
            existing = result.scalars().first()

            if existing:
                existing.open = kline[1]
                existing.high = kline[2]
                existing.low = kline[3]
                existing.close = kline[4]
                existing.volume = kline[5]
                existing.updated_at = func.now()
            else:
                new_record = cls(
                    trading_pair_id=trading_pair_id,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    open=kline[1],
                    high=kline[2],
                    low=kline[3],
                    close=kline[4],
                    volume=kline[5]
                )
                session.add(new_record)
                saved_count += 1

        await session.commit()
        return saved_count

    @classmethod
    async def get_latest_timestamp(cls, session, trading_pair_id, timeframe):
        """Get latest timestamp for a trading pair and timeframe"""
        query = select(func.max(cls.timestamp)) \
            .where(cls.trading_pair_id == trading_pair_id,
                   cls.timeframe == timeframe)
        result = await session.execute(query)
        return result.scalar()


class ExchangeApiKey(Base):
    """Exchange API keys model"""
    __tablename__ = 'exchange_api_keys'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    exchange_id = Column(Integer, ForeignKey('exchanges.id', ondelete='CASCADE'))
    name = Column(String(255), nullable=False)
    api_key = Column(Text, nullable=False)
    api_secret = Column(Text, nullable=True)
    passphrase = Column(Text, nullable=True)
    is_test_net = Column(Boolean, default=False)
    trading_enabled = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    permissions = Column(JSON, nullable=True)
    last_used_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('user_id', 'exchange_id', 'name'),
    )

    exchange = relationship("Exchange", back_populates="api_keys")


class OrderbookMetrics(Base):
    """Orderbook metrics model for detailed market data"""
    __tablename__ = 'orderbook_metrics'

    id = Column(Integer, primary_key=True)
    trading_pair_id = Column(Integer, ForeignKey('trading_pairs.id', ondelete='CASCADE'))
    timestamp = Column(DateTime, nullable=False)
    best_bid = Column(Numeric(24, 12), nullable=False)
    best_ask = Column(Numeric(24, 12), nullable=False)
    spread = Column(Numeric(24, 12), nullable=False)
    spread_percent = Column(Numeric(10, 6), nullable=False)
    mid_price = Column(Numeric(24, 12), nullable=False)
    bid_volume_10 = Column(Numeric(36, 12), nullable=False)  # Volume in top 10 bids
    ask_volume_10 = Column(Numeric(36, 12), nullable=False)  # Volume in top 10 asks
    bid_volume_50 = Column(Numeric(36, 12), nullable=False)  # Volume in top 50 bids
    ask_volume_50 = Column(Numeric(36, 12), nullable=False)  # Volume in top 50 asks
    total_bid_levels = Column(Integer, nullable=False)
    total_ask_levels = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('idx_orderbook_metrics_lookup', 'trading_pair_id', 'timestamp'),
        Index('idx_orderbook_metrics_recent', 'timestamp')
    )

    trading_pair = relationship("TradingPair")

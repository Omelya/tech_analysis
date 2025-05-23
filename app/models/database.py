from sqlalchemy import Column, Integer, String, DateTime, Numeric, Boolean, Text, JSON, ForeignKey, UniqueConstraint, Index
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

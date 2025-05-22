from pydantic_settings import BaseSettings
from typing import Dict, Any, Optional, List
import os


class Settings(BaseSettings):
    """Application settings configuration"""

    # FastAPI settings
    debug: bool = False
    host: str = "0.0.0.0"
    port: int = 8000

    # Database settings
    database_url: str = "mysql+aiomysql://user:password@localhost:3306/crypto_db"

    # Redis settings
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None

    # RabbitMQ settings
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Exchange API settings
    exchange_rate_limits: Dict[str, int] = {
        "binance": 1200,
        "bybit": 600,
        "whitebit": 300
    }

    # WebSocket settings
    websocket_max_connections: int = 100
    websocket_ping_interval: int = 20
    websocket_ping_timeout: int = 10

    # Data processing settings
    default_historical_limit: int = 1000
    max_historical_limit: int = 5000
    default_timeframes: List[str] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

    # Cache settings
    cache_ttl_ticker: int = 5
    cache_ttl_orderbook: int = 1
    cache_ttl_klines: int = 10
    cache_ttl_historical: int = 300

    # Monitoring settings
    prometheus_port: int = 9090
    log_level: str = "INFO"

    # Security settings
    api_key_encryption_key: str = "your-secret-key-here"
    allowed_origins: List[str] = ["*"]

    # Main service integration
    main_service_url: str = "http://localhost:8080"
    main_service_api_key: Optional[str] = None

    # MySQL specific settings
    mysql_charset: str = "utf8mb4"
    mysql_collation: str = "utf8mb4_unicode_ci"
    mysql_pool_size: int = 10
    mysql_max_overflow: int = 20

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

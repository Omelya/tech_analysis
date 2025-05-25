import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import structlog

from .config import settings
from .exchanges.manager import exchange_manager
from .api.routes import exchange_routes, admin_routes
from .utils.logging import setup_logging

# Import new stream processing components
from .stream_processing.stream_processor import StreamProcessor, StreamProcessorConfig
from .stream_processing.monitoring_system import monitoring_system
from .stream_processing.database_optimizer import initialize_database_optimizer


def create_app() -> FastAPI:
    """Create FastAPI application with stream processing integration"""

    setup_logging()

    app = FastAPI(
        title="High-Performance Crypto Data Microservice",
        description="Advanced microservice for processing cryptocurrency data with intelligent stream handling",
        version="3.0.0",
        debug=settings.debug
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API routers
    app.include_router(exchange_routes.router, prefix="/api/v1", tags=["exchanges"])
    app.include_router(admin_routes.router, prefix="/api/v1/admin", tags=["admin"])

    # Add stream routes
    from .api.routes.stream_routes import router as stream_router
    app.include_router(stream_router, prefix="/api/v1", tags=["streams"])

    @app.on_event("startup")
    async def startup_event():
        """Initialize all systems with new stream processing architecture"""
        logger = structlog.get_logger()
        logger.info("Starting High-Performance Crypto Data Microservice v3.0")

        try:
            # 1. Initialize monitoring system first
            await monitoring_system.initialize()
            logger.info("Monitoring system initialized")

            # 2. Initialize database migrations and optimizer
            from .models.migrations import migrator
            await migrator.initialize()
            await migrator.ensure_tables_exist()
            await migrator.add_indexes()

            # Initialize database optimizer
            await initialize_database_optimizer(settings.database_url)
            logger.info("Database optimizer initialized")

            # 3. Initialize exchange manager
            await exchange_manager.initialize()
            logger.info("Exchange manager initialized")

            # 4. Create and initialize stream processor
            stream_config = StreamProcessorConfig({
                'database_url': settings.database_url,
                'performance_mode': 'medium',
                'buffer_settings': {
                    'ticker': {'max_size': 10, 'max_age_ms': 500},
                    'orderbook': {'max_size': 5, 'max_age_ms': 200},
                    'klines': {'max_size': 2, 'max_age_ms': 1000},
                    'trades': {'max_size': 50, 'max_age_ms': 100}
                },
                'worker_settings': {
                    'fast': {'min': 2, 'max': 4},
                    'medium': {'min': 2, 'max': 3},
                    'slow': {'min': 1, 'max': 2}
                }
            })

            app.state.stream_processor = StreamProcessor(stream_config.__dict__)
            await app.state.stream_processor.initialize()
            logger.info("Stream processor initialized")

            # 5. Start WebSocket integrations
            from .services.websocket_server import websocket_server
            asyncio.create_task(websocket_server.start_server(port=settings.websocket_port))
            logger.info("WebSocket server started")

            # 6. Setup data flows
            await setup_data_flows(app.state.stream_processor)
            logger.info("Data flows configured")

            logger.info("High-Performance Crypto Data Microservice startup complete")

        except Exception as e:
            logger.error("Failed to initialize application", error=str(e))
            raise

    @app.on_event("shutdown")
    async def shutdown_event():
        """Graceful shutdown with stream processing cleanup"""
        logger = structlog.get_logger()
        logger.info("Shutting down High-Performance Crypto Data Microservice")

        try:
            # 1. Stop stream processor
            if hasattr(app.state, 'stream_processor'):
                await app.state.stream_processor.shutdown()
                logger.info("Stream processor shutdown complete")

            # 2. Stop WebSocket server
            from .services.websocket_server import websocket_server
            await websocket_server.stop_server()
            logger.info("WebSocket server stopped")

            # 3. Shutdown exchange manager
            await exchange_manager.shutdown()
            logger.info("Exchange manager shutdown")

            # 4. Shutdown monitoring
            await monitoring_system.shutdown()
            logger.info("Monitoring system shutdown")

            # 5. Close database connections
            from .models.migrations import migrator
            await migrator.close()
            logger.info("Database connections closed")

        except Exception as e:
            logger.error("Error during shutdown", error=str(e))

        logger.info("Application shutdown complete")

    @app.get("/")
    async def root():
        """Root endpoint with system information"""
        if not hasattr(app.state, 'stream_processor'):
            return {"error": "Stream processor not initialized"}

        try:
            stats = app.state.stream_processor.get_stats()
            health = await app.state.stream_processor.health_check()

            return {
                "service": "High-Performance Crypto Data Microservice",
                "version": "3.0.0",
                "status": health['overall'],
                "features": [
                    "intelligent_stream_processing",
                    "adaptive_buffer_management",
                    "dynamic_task_queuing",
                    "smart_backpressure_control",
                    "database_optimization",
                    "comprehensive_monitoring",
                    "circuit_breaker_protection",
                    "auto_scaling_workers"
                ],
                "performance": {
                    "uptime_seconds": stats['uptime_seconds'],
                    "messages_processed": stats['total_messages_processed'],
                    "processing_rate": stats.get('processing_rate', 0),
                    "drop_rate_percent": stats.get('drop_rate_percent', 0)
                },
                "supported_exchanges": exchange_manager.get_supported_exchanges(),
                "websocket_port": settings.websocket_port
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to get root status", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to get system status")

    @app.get("/health")
    async def health_check():
        """Comprehensive health check"""
        try:
            if not hasattr(app.state, 'stream_processor'):
                return {"status": "unhealthy", "error": "Stream processor not initialized"}

            # Get comprehensive health from stream processor
            health_data = await app.state.stream_processor.health_check()

            # Add exchange status
            exchange_status = await exchange_manager.get_all_exchanges_status()
            health_data['components']['exchanges'] = {
                'healthy': exchange_status.get('total_exchanges', 0) > 0,
                'details': exchange_status
            }

            # Add monitoring status
            monitoring_status = monitoring_system.get_comprehensive_status()
            health_data['components']['monitoring'] = {
                'healthy': monitoring_status.get('health_score', 0) > 50,
                'details': monitoring_status
            }

            return health_data

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Health check failed", error=str(e))
            raise HTTPException(status_code=500, detail="Health check failed")

    @app.get("/metrics")
    async def get_comprehensive_metrics():
        """Get comprehensive system metrics"""
        try:
            if not hasattr(app.state, 'stream_processor'):
                return {"error": "Stream processor not initialized"}

            # Get metrics from all systems
            stream_stats = app.state.stream_processor.get_stats()
            monitoring_status = monitoring_system.get_comprehensive_status()
            exchange_status = await exchange_manager.get_all_exchanges_status()

            return {
                "status": "success",
                "timestamp": monitoring_status.get('timestamp'),
                "data": {
                    "stream_processor": stream_stats,
                    "monitoring": monitoring_status,
                    "exchanges": exchange_status,
                    "system_health_score": monitoring_status.get('health_score', 0)
                }
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to get metrics", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to get metrics")

    # New endpoints for stream processing control
    @app.post("/api/v1/stream/process")
    async def process_stream_data(exchange: str, symbol: str, message_type: str, data: dict):
        """Process WebSocket message through stream processor"""
        try:
            if not hasattr(app.state, 'stream_processor'):
                raise HTTPException(status_code=503, detail="Stream processor not available")

            success = await app.state.stream_processor.process_message(
                exchange, symbol, message_type, data
            )

            return {
                "status": "success" if success else "throttled",
                "processed": success
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to process stream data", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to process stream data")

    @app.get("/api/v1/stream/stats")
    async def get_stream_stats():
        """Get detailed stream processing statistics"""
        try:
            if not hasattr(app.state, 'stream_processor'):
                raise HTTPException(status_code=503, detail="Stream processor not available")

            return {
                "status": "success",
                "data": app.state.stream_processor.get_stats()
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to get stream stats", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to get stream stats")

    @app.post("/api/v1/emergency/throttle")
    async def emergency_throttle(throttle_percent: int = 90):
        """Apply emergency throttling"""
        try:
            if not hasattr(app.state, 'stream_processor'):
                raise HTTPException(status_code=503, detail="Stream processor not available")

            # Apply throttling through backpressure controller
            backpressure = app.state.stream_processor.backpressure_controller

            # Set high throttle level
            from app.stream_processing.backpressure_controller import ThrottleLevel
            if throttle_percent >= 90:
                backpressure.throttle_level = ThrottleLevel.SEVERE
            elif throttle_percent >= 75:
                backpressure.throttle_level = ThrottleLevel.HEAVY
            elif throttle_percent >= 50:
                backpressure.throttle_level = ThrottleLevel.MODERATE
            else:
                backpressure.throttle_level = ThrottleLevel.LIGHT

            return {
                "status": "success",
                "message": f"Emergency throttle applied at {throttle_percent}%",
                "throttle_level": backpressure.throttle_level.name
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to apply emergency throttle", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to apply emergency throttle")

    return app


async def setup_data_flows(stream_processor: StreamProcessor):
    """Setup data flows between components"""
    logger = structlog.get_logger()

    try:
        from .models.database import TradingPair
        from .stream_processing.database_optimizer import database_optimizer

        if database_optimizer:
            async with database_optimizer._get_session() as session:
                active_pairs = await TradingPair.get_active_pairs(session)
                logger.info("Loaded active trading pairs", count=len(active_pairs))

        await setup_websocket_integration(stream_processor)

        logger.info("Data flows setup complete")

    except Exception as e:
        logger.error("Failed to setup data flows", error=str(e))
        raise


async def setup_websocket_integration(stream_processor: StreamProcessor):
    """Setup WebSocket message routing to stream processor"""

    # This function will be called by WebSocket handlers to route messages
    async def route_websocket_message(exchange: str, symbol: str, message_type: str, data: dict):
        """Route WebSocket message to stream processor"""
        try:
            await stream_processor.process_message(exchange, symbol, message_type, data)
        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to route WebSocket message",
                         exchange=exchange, symbol=symbol,
                         message_type=message_type, error=str(e))

    # Store routing function for use by WebSocket handlers
    from .services.websocket_server import websocket_server
    websocket_server.message_router = route_websocket_message


# Create app instance
app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )

import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import structlog

from .config import settings
from .exchanges.manager import exchange_manager
from .api.routes import exchange_routes, admin_routes
from .utils.logging import setup_logging


def create_app() -> FastAPI:
    """Create FastAPI application instance with full optimization"""

    setup_logging()

    app = FastAPI(
        title="Crypto Data Microservice",
        description="Optimized microservice for fetching and processing cryptocurrency data",
        version="2.0.0",
        debug=settings.debug
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include all routers
    app.include_router(exchange_routes.router, prefix="/api/v1", tags=["exchanges"])
    app.include_router(admin_routes.router, prefix="/api/v1/admin", tags=["admin"])

    # Add stream routes
    from .api.routes.stream_routes import router as stream_router
    app.include_router(stream_router, prefix="/api/v1", tags=["streams"])

    # Add optimized historical routes
    from .api.routes.historical_routes import router as historical_router
    app.include_router(historical_router, prefix="/api/v1", tags=["historical-optimized"])

    @app.on_event("startup")
    async def startup_event():
        """Initialize all services with optimization"""
        logger = structlog.get_logger()
        logger.info("Starting optimized crypto data microservice v2.0")

        # 1. Initialize database migrations
        from .models.migrations import migrator
        await migrator.initialize()
        await migrator.ensure_tables_exist()
        await migrator.add_indexes()

        # 2. Initialize exchange manager
        await exchange_manager.initialize()

        # 3. Initialize rate limit optimizer
        logger.info("Rate limit optimizer initialized")

        # 4. Initialize smart request scheduler
        from .services.request_scheduler import request_scheduler
        await request_scheduler.initialize()

        # 5. Initialize optimized historical data service
        from .services.historical_data_service import historical_data_service
        from .services.data_processor import data_processor
        await data_processor.initialize()
        await historical_data_service.initialize(data_processor.session_factory)

        # 6. Initialize stream integration
        from .services.stream_integration import stream_integration
        await stream_integration.initialize()

        # 7. Initialize WebSocket server
        from .services.websocket_server import websocket_server
        asyncio.create_task(websocket_server.start_server(port=settings.websocket_port))

        # 8. Start all processing services
        await data_processor.start_automatic_processing()
        await stream_integration.start_real_time_streams()

        logger.info("Optimized application startup complete - all systems operational")

    @app.on_event("shutdown")
    async def shutdown_event():
        """Graceful shutdown with optimization cleanup"""
        logger = structlog.get_logger()
        logger.info("Shutting down optimized crypto data microservice")

        # Stop services in reverse order
        try:
            # 1. Stop stream integration
            from .services.stream_integration import stream_integration
            await stream_integration.shutdown()

            # 2. Stop WebSocket server
            from .services.websocket_server import websocket_server
            await websocket_server.stop_server()

            # 3. Stop optimized historical service
            from .services.historical_data_service import historical_data_service
            await historical_data_service.shutdown()

            # 4. Stop smart scheduler
            from .services.request_scheduler import smart_scheduler
            await smart_scheduler.shutdown()

            # 5. Stop data processor
            from .services.data_processor import data_processor
            await data_processor.stop_all_tasks()

            # 6. Shutdown exchange manager
            await exchange_manager.shutdown()

            # 7. Close database connections
            from .models.migrations import migrator
            await migrator.close()

        except Exception as e:
            logger.error("Error during shutdown", error=str(e))

        logger.info("Optimized application shutdown complete")

    @app.get("/")
    async def root():
        """Root endpoint with optimization info"""
        from app.utils.rate_limiter import rate_limiter

        optimization_status = {}
        for exchange in exchange_manager.get_supported_exchanges():
            capacity = rate_limiter.get_exchange_capacity(exchange)
            optimization_status[exchange] = {
                'capacity_remaining_pct': round(
                    (capacity.get('requests_remaining', 0) / 1200) * 100, 1
                ),
                'adaptive_delay_factor': capacity.get('adaptive_delay_factor', 1.0),
                'status': 'optimal' if capacity.get('requests_remaining', 0) > 500 else 'throttled'
            }

        return {
            "service": "Crypto Data Microservice",
            "version": "2.0.0",
            "status": "running",
            "optimization_enabled": True,
            "supported_exchanges": exchange_manager.get_supported_exchanges(),
            "websocket_port": 8001,
            "features": [
                "intelligent_rate_limiting",
                "smart_request_scheduling",
                "optimized_historical_fetching",
                "real_time_streaming",
                "laravel_integration"
            ],
            "exchange_optimization_status": optimization_status
        }

    @app.get("/health")
    async def health_check():
        """Comprehensive health check with optimization metrics"""
        try:
            # Basic service status
            exchange_status = await exchange_manager.get_all_exchanges_status()

            # WebSocket server status
            from .services.websocket_server import websocket_server
            ws_stats = websocket_server.get_server_stats()

            # Stream integration status
            from .services.stream_integration import stream_integration
            stream_stats = stream_integration.get_integration_stats()

            # Optimization status
            from app.utils.rate_limiter import rate_limiter
            from .services.request_scheduler import request_scheduler
            from .services.historical_data_service import historical_data_service

            optimization_stats = {
                'rate_limiter': rate_limiter.get_optimizer_stats(),
                'smart_scheduler': await request_scheduler.get_queue_stats(),
                'historical_service': await historical_data_service.get_optimization_stats()
            }

            # Overall health score
            health_score = 100.0
            issues = []

            # Check for issues
            if exchange_status.get('total_exchanges', 0) == 0:
                health_score -= 30
                issues.append("No exchanges connected")

            if not ws_stats.get('running', False):
                health_score -= 20
                issues.append("WebSocket server not running")

            if sum(opt['consecutive_rate_limits'] for opt in
                   optimization_stats['rate_limiter']['exchanges'].values()) > 5:
                health_score -= 15
                issues.append("Multiple rate limit issues")

            status = "healthy" if health_score >= 80 else "degraded" if health_score >= 50 else "unhealthy"

            return {
                "status": status,
                "health_score": round(health_score, 1),
                "issues": issues,
                "services": {
                    "exchanges": exchange_status,
                    "websocket": ws_stats,
                    "streams": stream_stats,
                    "optimization": optimization_stats,
                },
                "timestamp": exchange_status.get('timestamp')
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Health check failed", error=str(e))
            raise HTTPException(status_code=500, detail="Health check failed")

    @app.get("/metrics")
    async def get_comprehensive_metrics():
        """Get comprehensive service metrics"""
        try:
            from .utils.metrics_collector import metrics_collector
            from app.utils.rate_limiter import rate_limiter
            from .services.request_scheduler import request_scheduler
            from .services.historical_data_service import historical_data_service

            return {
                "status": "success",
                "data": {
                    "general_metrics": metrics_collector.get_metrics_summary(),
                    "optimization_metrics": {
                        "rate_limiting": rate_limiter.get_optimizer_stats(),
                        "smart_scheduling": await request_scheduler.get_queue_stats(),
                        "historical_optimization": await historical_data_service.get_optimization_stats()
                    }
                }
            }

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to get metrics", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to get metrics")

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    )

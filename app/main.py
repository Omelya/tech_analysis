import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import structlog

from .config import settings
from .exchanges.manager import exchange_manager
from .api.routes import exchange_routes
from .utils.logging import setup_logging


def create_app() -> FastAPI:
    """Create FastAPI application instance"""

    setup_logging()

    app = FastAPI(
        title="Crypto Data Microservice",
        description="Microservice for fetching and processing cryptocurrency data from exchanges",
        version="1.0.0",
        debug=settings.debug
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(exchange_routes.router, prefix="/api/v1", tags=["exchanges"])

    @app.on_event("startup")
    async def startup_event():
        """Initialize services on startup"""
        logger = structlog.get_logger()
        logger.info("Starting crypto data microservice")

        # Initialize exchange manager
        await exchange_manager.initialize()

        # Initialize data processor
        from .services.data_processor import data_processor
        await data_processor.initialize()

        # Start automatic data processing
        await data_processor.start_automatic_processing()

        logger.info("Application startup complete")

    @app.on_event("shutdown")
    async def shutdown_event():
        """Cleanup on shutdown"""
        logger = structlog.get_logger()
        logger.info("Shutting down crypto data microservice")

        # Stop data processor
        from .services.data_processor import data_processor
        await data_processor.stop_all_tasks()

        # Shutdown exchange manager
        await exchange_manager.shutdown()

        logger.info("Application shutdown complete")

    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "service": "Crypto Data Microservice",
            "version": "1.0.0",
            "status": "running",
            "supported_exchanges": exchange_manager.get_supported_exchanges()
        }

    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        try:
            status = await exchange_manager.get_all_exchanges_status()
            return {
                "status": "healthy",
                "exchanges": status
            }
        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Health check failed", error=str(e))
            raise HTTPException(status_code=500, detail="Service unhealthy")

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

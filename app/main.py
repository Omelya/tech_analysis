import asyncio
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, List

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import structlog

from .config import settings
from .exchanges.manager import exchange_manager
from .api.routes import exchange_routes, admin_routes
from .utils.logging import setup_logging

from .stream_processing.stream_processor import StreamProcessor, StreamProcessorConfig
from .stream_processing.monitoring_system import monitoring_system
from .stream_processing.database_optimizer import database_optimizer


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
        """Initialize all systems with new centralized WebSocket architecture"""
        logger = structlog.get_logger()
        logger.info("🚀 Starting High-Performance Crypto Data Microservice v3.0")

        try:
            # 1. Initialize monitoring system first
            await monitoring_system.initialize()
            logger.info("✅ Monitoring system initialized")

            # 2. Initialize database migrations and optimizer
            from .models.migrations import migrator
            await migrator.initialize()
            await migrator.ensure_tables_exist()
            await migrator.add_indexes()

            # Initialize database optimizer
            await database_optimizer.initialize()
            logger.info("✅ Database optimizer initialized")

            # 3. Initialize exchange manager with centralized WebSocket management
            await exchange_manager.initialize()
            logger.info("✅ Exchange manager initialized")

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
            logger.info("✅ Stream processor initialized")

            # 5. Start WebSocket server
            from .services.websocket_server import websocket_server
            asyncio.create_task(websocket_server.start_server(port=settings.websocket_port))
            logger.info("✅ WebSocket server started on port", port=settings.websocket_port)

            # 6. Setup centralized data flows with bulk subscriptions
            await setup_data_flows(app.state.stream_processor)
            logger.info("✅ Data flows configured")

            # 7. Start background monitoring and health checks
            asyncio.create_task(start_health_monitoring())
            logger.info("✅ Health monitoring started")

            # 8. Log final statistics
            await log_startup_statistics()

            logger.info("🎉 High-Performance Crypto Data Microservice startup complete!")

        except Exception as e:
            logger.error("💥 Failed to initialize application", error=str(e))
            raise

    async def start_health_monitoring():
        """Start background health monitoring tasks"""
        logger = structlog.get_logger()

        async def health_monitor_loop():
            """Continuous health monitoring"""
            while True:
                try:
                    # Check subscription health every 2 minutes
                    health = await get_subscription_health_status()

                    if health['overall_health'] != 'healthy':
                        logger.warning("Subscription health issues detected",
                                       overall_health=health['overall_health'],
                                       issues=health['issues'])

                        # Auto-recovery for critical issues
                        if health['overall_health'] == 'unhealthy':
                            logger.info("Attempting automatic recovery...")
                            await attempt_recovery()

                    # Log stats every 10 minutes
                    stats = await exchange_manager.get_subscription_stats()
                    logger.info("📊 Subscription statistics",
                                total_subscriptions=stats['total_subscriptions'],
                                exchanges={ex: info['count']
                                           for ex, info in stats['by_exchange'].items()})

                    await asyncio.sleep(120)  # Check every 2 minutes

                except Exception as e:
                    logger.error("Health monitoring error", error=str(e))
                    await asyncio.sleep(300)  # Wait 5 minutes on error

        # Start monitoring task
        asyncio.create_task(health_monitor_loop())

    async def attempt_recovery():
        """Attempt to recover unhealthy connections"""
        logger = structlog.get_logger()

        try:
            # Get current health status
            health = await get_subscription_health_status()

            for exchange, info in health['exchanges'].items():
                if info['status'] == 'unhealthy':
                    logger.info("🔄 Attempting recovery for exchange", exchange=exchange)

                    # Try to reinitialize the exchange adapter
                    adapter = await exchange_manager.get_public_adapter(exchange)
                    if adapter:
                        # Force reconnection
                        await adapter.websocket._reconnect()
                        logger.info("✅ Recovery attempted for exchange", exchange=exchange)

            # Wait a bit and check again
            await asyncio.sleep(30)

            post_recovery_health = await get_subscription_health_status()
            logger.info("Recovery results",
                        pre_recovery=health['overall_health'],
                        post_recovery=post_recovery_health['overall_health'])

        except Exception as e:
            logger.error("Recovery attempt failed", error=str(e))

    async def log_startup_statistics():
        """Log comprehensive startup statistics"""
        logger = structlog.get_logger()

        try:
            # Subscription statistics
            sub_stats = await exchange_manager.get_subscription_stats()

            # System health
            health = await get_subscription_health_status()

            # Stream processor stats
            stream_stats = app.state.stream_processor.get_stats() if hasattr(app.state, 'stream_processor') else {}

            startup_stats = {
                "🔗 WebSocket Subscriptions": {
                    "Total": sub_stats['total_subscriptions'],
                    "By Exchange": {ex: info['count'] for ex, info in sub_stats['by_exchange'].items()},
                    "Health": health['overall_health']
                },
                "⚡ Stream Processor": {
                    "Performance Mode": stream_stats.get('performance_mode', 'unknown'),
                    "Components Running": len([comp for comp, stats in stream_stats.items()
                                               if isinstance(stats, dict) and stats.get('running', False)])
                },
                "🌐 WebSocket Connections": {
                    exchange: conn_info['connected']
                    for exchange, conn_info in sub_stats.get('websocket_connections', {}).items()
                }
            }

            logger.info("📈 Startup Statistics", **startup_stats)

            # Performance recommendations
            recommendations = monitoring_system.get_performance_recommendations()
            if recommendations:
                logger.info("💡 Performance Recommendations",
                            recommendations=[r['title'] for r in recommendations[:3]])

        except Exception as e:
            logger.error("Failed to log startup statistics", error=str(e))

    @app.on_event("shutdown")
    async def shutdown_event():
        """Enhanced graceful shutdown with proper WebSocket cleanup"""
        logger = structlog.get_logger()
        logger.info("🛑 Shutting down High-Performance Crypto Data Microservice")

        try:
            # 1. Stop accepting new subscriptions
            logger.info("🔒 Stopping new subscriptions...")

            # 2. Gracefully close all WebSocket subscriptions
            logger.info("📡 Closing WebSocket subscriptions...")
            sub_stats = await exchange_manager.get_subscription_stats()
            total_subs = sub_stats['total_subscriptions']

            # 3. Stop stream processor
            if hasattr(app.state, 'stream_processor'):
                logger.info("⚙️ Stopping stream processor...")
                await app.state.stream_processor.shutdown()
                logger.info("✅ Stream processor shutdown complete")

            # 4. Stop WebSocket server
            from .services.websocket_server import websocket_server
            logger.info("🌐 Stopping WebSocket server...")
            await websocket_server.stop_server()
            logger.info("✅ WebSocket server stopped")

            # 5. Shutdown exchange manager (closes all WebSocket connections)
            logger.info("🔌 Shutting down exchange manager...")
            await exchange_manager.shutdown()
            logger.info(f"✅ Exchange manager shutdown ({total_subs} subscriptions closed)")

            # 6. Shutdown monitoring
            logger.info("📊 Stopping monitoring...")
            await monitoring_system.shutdown()
            logger.info("✅ Monitoring system shutdown")

            # 7. Close database connections
            from .models.migrations import migrator
            await migrator.close()
            logger.info("✅ Database connections closed")

            logger.info("🎯 Application shutdown complete - All connections properly closed")

        except Exception as e:
            logger.error("❌ Error during shutdown", error=str(e))

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
    async def enhanced_health_check():
        """Enhanced health check with WebSocket subscription status"""
        try:
            health_data = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'components': {},
                'websockets': {},
                'subscriptions': {}
            }

            # Stream processor health
            if hasattr(app.state, 'stream_processor'):
                stream_health = await app.state.stream_processor.health_check()
                health_data['components']['stream_processor'] = stream_health
            else:
                health_data['components']['stream_processor'] = {
                    'healthy': False,
                    'status': 'not_initialized'
                }

            # Exchange manager health
            exchange_status = await exchange_manager.get_all_exchanges_status()
            health_data['components']['exchanges'] = {
                'healthy': exchange_status.get('total_exchanges', 0) > 0,
                'details': exchange_status
            }

            # WebSocket subscription health
            subscription_health = await get_subscription_health_status()
            health_data['websockets'] = subscription_health['websocket_connections']
            health_data['subscriptions'] = {
                'total': subscription_health['total_subscriptions'],
                'health': subscription_health['overall_health'],
                'by_exchange': subscription_health['exchanges']
            }

            # Monitoring status
            monitoring_status = monitoring_system.get_comprehensive_status()
            health_data['components']['monitoring'] = {
                'healthy': monitoring_status.get('health_score', 0) > 50,
                'details': monitoring_status
            }

            # Determine overall health
            component_health = [
                health_data['components']['stream_processor'].get('healthy', False),
                health_data['components']['exchanges']['healthy'],
                health_data['components']['monitoring']['healthy'],
                subscription_health['overall_health'] in ['healthy', 'degraded']
            ]

            if all(component_health):
                health_data['status'] = 'healthy'
            elif sum(component_health) >= len(component_health) * 0.75:
                health_data['status'] = 'degraded'
            else:
                health_data['status'] = 'unhealthy'

            return health_data

        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Enhanced health check failed", error=str(e))
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    @app.get("/api/v1/subscriptions/stats")
    async def get_subscription_statistics():
        """Get detailed WebSocket subscription statistics"""
        try:
            stats = await exchange_manager.get_subscription_stats()
            health = await get_subscription_health_status()

            return {
                "status": "success",
                "data": {
                    "statistics": stats,
                    "health": health,
                    "timestamp": datetime.now().isoformat()
                }
            }
        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Failed to get subscription stats", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to get subscription statistics")

    @app.post("/api/v1/subscriptions/recover")
    async def trigger_subscription_recovery():
        """Manually trigger subscription recovery"""
        try:
            await attempt_recovery()

            # Get post-recovery status
            health = await get_subscription_health_status()

            return {
                "status": "success",
                "message": "Recovery attempted",
                "health_status": health['overall_health'],
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            logger = structlog.get_logger()
            logger.error("Manual recovery failed", error=str(e))
            raise HTTPException(status_code=500, detail="Recovery attempt failed")

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
    """Setup data flows between components with centralized WebSocket management"""
    logger = structlog.get_logger()

    try:
        from .models.database import TradingPair
        from .stream_processing.database_optimizer import database_optimizer

        if database_optimizer:
            async with database_optimizer._get_session() as session:
                active_pairs = await TradingPair.get_active_pairs(session)
                logger.info("Loaded active trading pairs", count=len(active_pairs))

                # Group trading pairs by exchange for efficient bulk subscription
                pairs_by_exchange = defaultdict(list)
                for pair in active_pairs:
                    exchange_slug = pair['exchange_slug']
                    pairs_by_exchange[exchange_slug].append(pair)

                # Setup bulk subscriptions per exchange
                for exchange_slug, pairs in pairs_by_exchange.items():
                    await setup_exchange_bulk_subscriptions(exchange_slug, pairs, stream_processor)

        # Setup WebSocket integration with stream processor
        await setup_websocket_integration(stream_processor)
        logger.info("Data flows setup complete")

    except Exception as e:
        logger.error("Failed to setup data flows", error=str(e))
        raise


async def setup_exchange_bulk_subscriptions(exchange_slug: str, pairs: List[Dict],
                                            stream_processor: StreamProcessor):
    """Setup bulk subscriptions for an exchange efficiently"""
    logger = structlog.get_logger()

    try:
        # Create callback for stream processor integration
        async def data_callback(data: Dict[str, Any]):
            """Process WebSocket data through stream processor"""
            try:
                await stream_processor.process_message(
                    exchange=exchange_slug,
                    symbol=data.get('symbol', ''),
                    message_type=data.get('type', 'ticker'),
                    data=data.get('data', {})
                )
            except Exception as e:
                logger.error("Failed to process WebSocket data",
                             exchange=exchange_slug, error=str(e))

        subscriptions = []

        for pair in pairs:
            symbol = pair['symbol']

            essential_streams = [
                {
                    'exchange': exchange_slug,
                    'symbol': symbol,
                    'stream_type': 'ticker',
                    'callback': data_callback
                },
                {
                    'exchange': exchange_slug,
                    'symbol': symbol,
                    'stream_type': 'orderbook',
                    'callback': data_callback
                }
            ]

            key_timeframes = ['1m', '5m', '15m', '1h', '4h', '1d']
            for timeframe in key_timeframes:
                essential_streams.append({
                    'exchange': exchange_slug,
                    'symbol': symbol,
                    'stream_type': 'klines',
                    'timeframe': timeframe,
                    'callback': data_callback
                })

            subscriptions.extend(essential_streams)

        logger.info("Starting bulk subscription",
                    exchange=exchange_slug,
                    pairs=len(pairs),
                    total_subscriptions=len(subscriptions))

        results = await exchange_manager.bulk_subscribe(subscriptions)

        logger.info("Bulk subscription completed",
                    exchange=exchange_slug,
                    successful=len(results['successful']),
                    failed=len(results['failed']))

        # Log failures for debugging
        if results['failed']:
            for failure in results['failed'][:5]:  # Log first 5 failures
                logger.warning("Subscription failed",
                               exchange=exchange_slug,
                               symbol=failure['subscription']['symbol'],
                               stream_type=failure['subscription']['stream_type'],
                               error=failure['error'])

    except Exception as e:
        logger.error("Failed to setup bulk subscriptions",
                     exchange=exchange_slug, error=str(e))


async def setup_websocket_integration(stream_processor: StreamProcessor):
    """Setup WebSocket message routing to stream processor"""
    logger = structlog.get_logger()

    try:
        # Setup WebSocket server integration
        from .services.websocket_server import websocket_server

        # Create routing function for WebSocket messages
        async def route_websocket_message(exchange: str, symbol: str, message_type: str, data: dict):
            """Route WebSocket message to stream processor"""
            try:
                await stream_processor.process_message(exchange, symbol, message_type, data)
            except Exception as e:
                logger.error("Failed to route WebSocket message",
                             exchange=exchange, symbol=symbol,
                             message_type=message_type, error=str(e))

        # Store routing function for use by WebSocket handlers
        websocket_server.message_router = route_websocket_message

        logger.info("WebSocket integration setup complete")

    except Exception as e:
        logger.error("Failed to setup WebSocket integration", error=str(e))
        raise


async def add_trading_pair_subscriptions(exchange_slug: str, symbol: str,
                                         stream_processor: StreamProcessor) -> Dict[str, Any]:
    """Add subscriptions for a new trading pair"""
    logger = structlog.get_logger()

    async def data_callback(data: Dict[str, Any]):
        await stream_processor.process_message(
            exchange=exchange_slug,
            symbol=symbol,
            message_type=data.get('type', 'ticker'),
            data=data.get('data', {})
        )

    subscriptions = [
        {
            'exchange': exchange_slug,
            'symbol': symbol,
            'stream_type': 'ticker',
            'callback': data_callback
        },
        {
            'exchange': exchange_slug,
            'symbol': symbol,
            'stream_type': 'orderbook',
            'callback': data_callback
        }
    ]

    # Add klines for key timeframes
    for timeframe in ['1m', '5m', '1h', '1d']:
        subscriptions.append({
            'exchange': exchange_slug,
            'symbol': symbol,
            'stream_type': 'klines',
            'timeframe': timeframe,
            'callback': data_callback
        })

    results = await exchange_manager.bulk_subscribe(subscriptions)

    logger.info("Added subscriptions for new trading pair",
                exchange=exchange_slug, symbol=symbol,
                successful=len(results['successful']),
                failed=len(results['failed']))

    return results


async def remove_trading_pair_subscriptions(exchange_slug: str, symbol: str) -> Dict[str, Any]:
    """Remove subscriptions for a trading pair"""
    logger = structlog.get_logger()

    try:
        # Get current subscriptions
        stats = await exchange_manager.get_subscription_stats()
        exchange_subscriptions = stats.get('by_exchange', {}).get(exchange_slug, {}).get('subscriptions', [])

        # Find subscriptions for this symbol
        symbol_subscriptions = [
            sub for sub in exchange_subscriptions
            if f":{symbol}:" in sub
        ]

        # Remove subscriptions
        removal_results = []
        for subscription_key in symbol_subscriptions:
            success = await exchange_manager.unsubscribe_from_stream(subscription_key)
            removal_results.append({
                'subscription_key': subscription_key,
                'success': success
            })

        successful_removals = sum(1 for r in removal_results if r['success'])

        logger.info("Removed subscriptions for trading pair",
                    exchange=exchange_slug, symbol=symbol,
                    total_subscriptions=len(symbol_subscriptions),
                    successful_removals=successful_removals)

        return {
            'total_subscriptions': len(symbol_subscriptions),
            'successful_removals': successful_removals,
            'failed_removals': len(symbol_subscriptions) - successful_removals,
            'details': removal_results
        }

    except Exception as e:
        logger.error("Failed to remove subscriptions",
                     exchange=exchange_slug, symbol=symbol, error=str(e))
        return {
            'error': str(e),
            'total_subscriptions': 0,
            'successful_removals': 0,
            'failed_removals': 0
        }


async def get_subscription_health_status() -> Dict[str, Any]:
    """Get comprehensive subscription health status"""
    try:
        stats = await exchange_manager.get_subscription_stats()

        health_status = {
            'overall_health': 'healthy',
            'total_subscriptions': stats['total_subscriptions'],
            'exchanges': {},
            'issues': []
        }

        # Check each exchange
        for exchange, info in stats['by_exchange'].items():
            ws_info = stats['websocket_connections'][exchange]

            exchange_health = {
                'subscriptions': info['count'],
                'websocket_connected': ws_info['connected'],
                'adapter_available': ws_info['adapter_available'],
                'status': 'healthy'
            }

            # Determine exchange health
            if not ws_info['connected'] or not ws_info['adapter_available']:
                exchange_health['status'] = 'unhealthy'
                health_status['issues'].append(f"{exchange}: WebSocket connection issues")
            elif info['count'] == 0:
                exchange_health['status'] = 'degraded'
                health_status['issues'].append(f"{exchange}: No active subscriptions")

            health_status['exchanges'][exchange] = exchange_health

        # Determine overall health
        unhealthy_exchanges = sum(1 for ex in health_status['exchanges'].values()
                                  if ex['status'] == 'unhealthy')

        if unhealthy_exchanges > 0:
            health_status['overall_health'] = 'degraded' if unhealthy_exchanges == 1 else 'unhealthy'

        return health_status

    except Exception as e:
        return {
            'overall_health': 'unhealthy',
            'error': str(e),
            'total_subscriptions': 0,
            'exchanges': {},
            'issues': ['Failed to get health status']
        }

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

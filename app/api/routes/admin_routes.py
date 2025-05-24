from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
import structlog

from ...services.data_processor import data_processor
from ...services.orderbook_processor import orderbook_processor
from ...services.historical_data_service import historical_data_service, TaskPriority
from ...services.websocket_server import websocket_server
from ...exchanges.manager import exchange_manager
from ...utils.metrics_collector import metrics_collector
from ...models.migrations import migrator


class HistoricalDataRequest(BaseModel):
    exchange_slug: str
    symbol: str
    timeframe: str
    priority: str = "medium"
    since: Optional[int] = None
    limit: int = 1000


class BulkImportRequest(BaseModel):
    exchange_slug: str
    symbol: str
    timeframes: List[str]
    days_back: int = 30
    priority: str = "high"


router = APIRouter()
logger = structlog.get_logger().bind(component="admin_api")


@router.get("/status")
async def get_system_status():
    """Get overall system status"""
    try:
        # Get all service statuses
        exchange_status = await exchange_manager.get_all_exchanges_status()
        websocket_stats = websocket_server.get_server_stats()
        historical_stats = await historical_data_service.get_optimization_stats()
        metrics_summary = metrics_collector.get_metrics_summary()

        return {
            "status": "success",
            "timestamp": metrics_summary["timestamp"],
            "uptime_seconds": metrics_summary["uptime_seconds"],
            "services": {
                "exchanges": exchange_status,
                "websocket": websocket_stats,
                "historical_data": historical_stats,
                "data_processor": {
                    "active_tasks": len(data_processor.active_tasks),
                    "subscriptions": len(data_processor.subscriptions)
                },
                "orderbook_processor": {
                    "active_tasks": len(orderbook_processor.active_tasks),
                    "subscriptions": len(orderbook_processor.subscriptions)
                }
            },
            "metrics": metrics_summary
        }

    except Exception as e:
        logger.error("Failed to get system status", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get system status")


@router.get("/metrics")
async def get_metrics():
    """Get detailed metrics"""
    try:
        return {
            "status": "success",
            "data": metrics_collector.get_metrics_summary()
        }
    except Exception as e:
        logger.error("Failed to get metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.get("/metrics/{exchange}")
async def get_exchange_metrics(exchange: str):
    """Get metrics for specific exchange"""
    try:
        exchange_metrics = metrics_collector.get_exchange_metrics(exchange)

        if not exchange_metrics:
            raise HTTPException(status_code=404, detail=f"No metrics found for exchange {exchange}")

        return {
            "status": "success",
            "exchange": exchange,
            "data": exchange_metrics
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get exchange metrics", exchange=exchange, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get exchange metrics")


@router.post("/historical-data/schedule")
async def schedule_historical_data(request: HistoricalDataRequest):
    """Schedule historical data fetch task"""
    try:
        # Validate priority
        priority_map = {
            "low": TaskPriority.LOW,
            "medium": TaskPriority.MEDIUM,
            "high": TaskPriority.HIGH,
            "critical": TaskPriority.CRITICAL
        }

        priority = priority_map.get(request.priority.lower(), TaskPriority.MEDIUM)

        task_id = historical_data_service.schedule_task(
            exchange_slug=request.exchange_slug,
            symbol=request.symbol,
            timeframe=request.timeframe,
            priority=priority,
            since=request.since,
            limit=request.limit
        )

        return {
            "status": "success",
            "message": "Historical data task scheduled",
            "task_id": task_id,
            "priority": request.priority
        }

    except Exception as e:
        logger.error("Failed to schedule historical data task", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to schedule task")


@router.post("/historical-data/bulk-import")
async def bulk_import_historical_data(request: BulkImportRequest):
    """Schedule bulk historical data import"""
    try:
        priority_map = {
            "low": TaskPriority.LOW,
            "medium": TaskPriority.MEDIUM,
            "high": TaskPriority.HIGH,
            "critical": TaskPriority.CRITICAL
        }

        priority = priority_map.get(request.priority.lower(), TaskPriority.HIGH)

        task_ids = historical_data_service.schedule_bulk_import(
            exchange_slug=request.exchange_slug,
            symbol=request.symbol,
            timeframes=request.timeframes,
            days_back=request.days_back,
            priority=priority
        )

        return {
            "status": "success",
            "message": "Bulk import tasks scheduled",
            "task_ids": task_ids,
            "tasks_count": len(task_ids)
        }

    except Exception as e:
        logger.error("Failed to schedule bulk import", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to schedule bulk import")


@router.post("/historical-data/detect-gaps/{exchange}/{symbol}/{timeframe}")
async def detect_and_fill_gaps(exchange: str, symbol: str, timeframe: str):
    """Detect and fill data gaps"""
    try:
        task_ids = await historical_data_service.detect_and_fill_gaps(exchange, symbol, timeframe)

        return {
            "status": "success",
            "message": "Gap detection completed",
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "gap_fill_tasks": task_ids,
            "gaps_found": len(task_ids)
        }

    except Exception as e:
        logger.error("Failed to detect gaps", exchange=exchange, symbol=symbol,
                     timeframe=timeframe, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to detect gaps")


@router.get("/historical-data/validate/{exchange}/{symbol}/{timeframe}")
async def validate_data_integrity(exchange: str, symbol: str, timeframe: str,
                                  days_back: int = Query(7, ge=1, le=30)):
    """Validate data integrity"""
    try:
        validation_result = await historical_data_service.validate_data_integrity(
            exchange, symbol, timeframe, days_back
        )

        return {
            "status": "success",
            "data": validation_result
        }

    except Exception as e:
        logger.error("Failed to validate data integrity", exchange=exchange,
                     symbol=symbol, timeframe=timeframe, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to validate data integrity")


@router.get("/websocket/stats")
async def get_websocket_stats():
    """Get WebSocket server statistics"""
    try:
        stats = websocket_server.get_server_stats()
        return {
            "status": "success",
            "data": stats
        }
    except Exception as e:
        logger.error("Failed to get WebSocket stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get WebSocket stats")


@router.get("/database/stats")
async def get_database_stats():
    """Get database statistics"""
    try:
        stats = await migrator.get_table_stats()
        return {
            "status": "success",
            "data": {
                "tables": stats,
                "total_size_mb": sum(table["size_mb"] for table in stats),
                "total_rows": sum(table["rows"] for table in stats)
            }
        }
    except Exception as e:
        logger.error("Failed to get database stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get database stats")


@router.post("/database/optimize")
async def optimize_database():
    """Optimize database tables"""
    try:
        success = await migrator.optimize_tables()

        if success:
            return {
                "status": "success",
                "message": "Database optimization completed"
            }
        else:
            return {
                "status": "error",
                "message": "Database optimization failed"
            }
    except Exception as e:
        logger.error("Failed to optimize database", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to optimize database")


@router.post("/system/restart-service/{service}")
async def restart_service(service: str):
    """Restart specific service component"""
    try:
        if service == "data_processor":
            # Stop current processing
            await data_processor.stop_all_tasks()

            # Restart processing
            await data_processor.start_automatic_processing()

            message = "Data processor restarted"

        elif service == "orderbook_processor":
            # Stop orderbook processing
            await orderbook_processor.stop_all_tasks()

            # Restart with active pairs
            active_pairs = await data_processor.get_active_trading_pairs()
            await orderbook_processor.start_processing(active_pairs)

            message = "Orderbook processor restarted"

        elif service == "historical_service":
            # Restart historical data service
            await historical_data_service.shutdown()
            await historical_data_service.initialize(data_processor.session_factory)

            message = "Historical data service restarted"

        else:
            raise HTTPException(status_code=400, detail=f"Unknown service: {service}")

        return {
            "status": "success",
            "message": message,
            "service": service
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to restart service", service=service, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to restart {service}")


@router.get("/system/health")
async def detailed_health_check():
    """Detailed system health check"""
    try:
        health_status = {
            "overall": "healthy",
            "components": {},
            "timestamp": metrics_collector.get_metrics_summary()["timestamp"]
        }

        # Check exchange connections
        exchange_status = await exchange_manager.get_all_exchanges_status()
        health_status["components"]["exchanges"] = {
            "status": "healthy" if exchange_status["total_exchanges"] > 0 else "unhealthy",
            "details": exchange_status
        }

        # Check WebSocket server
        ws_stats = websocket_server.get_server_stats()
        health_status["components"]["websocket"] = {
            "status": "healthy" if ws_stats["running"] else "unhealthy",
            "details": ws_stats
        }

        # Check historical data service
        hist_stats = historical_data_service.get_service_stats()
        health_status["components"]["historical_data"] = {
            "status": "healthy" if hist_stats["running"] else "unhealthy",
            "details": hist_stats
        }

        # Check database
        try:
            db_stats = await migrator.get_table_stats()
            health_status["components"]["database"] = {
                "status": "healthy" if db_stats else "unhealthy",
                "details": {"tables_count": len(db_stats)}
            }
        except Exception:
            health_status["components"]["database"] = {
                "status": "unhealthy",
                "details": {"error": "Database connection failed"}
            }

        # Check overall health
        unhealthy_components = [
            comp for comp, data in health_status["components"].items()
            if data["status"] == "unhealthy"
        ]

        if unhealthy_components:
            health_status["overall"] = "degraded" if len(unhealthy_components) == 1 else "unhealthy"
            health_status["unhealthy_components"] = unhealthy_components

        return {
            "status": "success",
            "data": health_status
        }

    except Exception as e:
        logger.error("Failed to perform health check", error=str(e))
        raise HTTPException(status_code=500, detail="Health check failed")


@router.get("/system/logs")
async def get_recent_logs(
        level: str = Query("INFO", regex="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$"),
        limit: int = Query(100, ge=1, le=1000)
):
    """Get recent log entries (Note: This is a placeholder - actual implementation would depend on log storage)"""
    try:
        # This is a placeholder implementation
        # In a real system, you would query your log storage system
        return {
            "status": "success",
            "message": "Log retrieval not implemented - logs are available via external log aggregation system",
            "suggestion": "Use structured logging output or external log management system"
        }
    except Exception as e:
        logger.error("Failed to get logs", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get logs")


@router.post("/metrics/reset")
async def reset_metrics():
    """Reset all metrics (useful for testing)"""
    try:
        metrics_collector.reset_metrics()

        return {
            "status": "success",
            "message": "All metrics have been reset"
        }
    except Exception as e:
        logger.error("Failed to reset metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to reset metrics")


@router.get("/trading-pairs/active")
async def get_active_trading_pairs():
    """Get list of active trading pairs"""
    try:
        active_pairs = await data_processor.get_active_trading_pairs()

        return {
            "status": "success",
            "data": active_pairs,
            "count": len(active_pairs)
        }
    except Exception as e:
        logger.error("Failed to get active trading pairs", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get active trading pairs")


@router.get("/config")
async def get_system_config():
    """Get system configuration (sanitized)"""
    try:
        from ...config import settings

        config = {
            "debug": settings.debug,
            "host": settings.host,
            "port": settings.port,
            "redis_host": settings.redis_host,
            "redis_port": settings.redis_port,
            "exchange_rate_limits": settings.exchange_rate_limits,
            "websocket_max_connections": settings.websocket_max_connections,
            "default_timeframes": settings.default_timeframes,
            "cache_ttl": {
                "ticker": settings.cache_ttl_ticker,
                "orderbook": settings.cache_ttl_orderbook,
                "klines": settings.cache_ttl_klines,
                "historical": settings.cache_ttl_historical
            }
        }

        return {
            "status": "success",
            "data": config
        }
    except Exception as e:
        logger.error("Failed to get system config", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get system config")

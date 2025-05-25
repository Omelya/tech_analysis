from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Body, Request
from pydantic import BaseModel
import structlog

from ...exchanges.manager import exchange_manager
from ...services.websocket_server import websocket_server
from ...models.migrations import migrator
from ...stream_processing.monitoring_system import monitoring_system
from ...stream_processing.backpressure_controller import backpressure_controller
from ...stream_processing.task_queue import task_queue
from ...stream_processing.buffer_manager import buffer_manager
from ...stream_processing.database_optimizer import database_optimizer


class EmergencyActionRequest(BaseModel):
    action: str  # "throttle", "stop", "restart"
    throttle_percent: Optional[int] = 90
    reason: Optional[str] = None


class SystemConfigRequest(BaseModel):
    performance_mode: Optional[str] = None  # "light", "medium", "heavy"
    buffer_settings: Optional[Dict[str, Any]] = None
    worker_settings: Optional[Dict[str, Any]] = None


router = APIRouter()
logger = structlog.get_logger().bind(component="admin_api")


@router.get("/status")
async def get_system_status(request: Request):
    """Get comprehensive system status"""
    try:
        # Get stream processor if available
        stream_processor = None
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor

        # Collect status from all systems
        status_data = {
            "timestamp": monitoring_system.get_comprehensive_status().get('timestamp'),
            "services": {},
            "overall_health": "unknown"
        }

        # Exchange manager status
        exchange_status = await exchange_manager.get_all_exchanges_status()
        status_data["services"]["exchanges"] = {
            "healthy": exchange_status.get('total_exchanges', 0) > 0,
            "details": exchange_status
        }

        # WebSocket server status
        ws_stats = websocket_server.get_server_stats()
        status_data["services"]["websocket"] = {
            "healthy": ws_stats["running"],
            "details": ws_stats
        }

        # Monitoring system status
        monitoring_status = monitoring_system.get_comprehensive_status()
        status_data["services"]["monitoring"] = {
            "healthy": monitoring_status.get('health_score', 0) > 50,
            "details": monitoring_status
        }

        # Stream processing components
        if stream_processor:
            stream_health = await stream_processor.health_check()
            status_data["services"]["stream_processor"] = {
                "healthy": stream_health["overall"] in ["healthy", "degraded"],
                "details": stream_health
            }

        # Buffer manager status
        buffer_stats = buffer_manager.get_stats()
        status_data["services"]["buffer_manager"] = {
            "healthy": buffer_stats["running"],
            "details": buffer_stats
        }

        # Task queue status
        queue_stats = task_queue.get_stats()
        status_data["services"]["task_queue"] = {
            "healthy": queue_stats["running"],
            "details": queue_stats
        }

        # Backpressure controller status
        bp_stats = backpressure_controller.get_stats()
        status_data["services"]["backpressure_controller"] = {
            "healthy": bp_stats["running"],
            "details": bp_stats
        }

        # Database optimizer status
        if database_optimizer:
            db_health = await database_optimizer.health_check()
            db_stats = database_optimizer.get_stats()
            status_data["services"]["database_optimizer"] = {
                "healthy": db_health,
                "details": db_stats
            }

        # Calculate overall health
        healthy_services = sum(1 for service in status_data["services"].values() if service["healthy"])
        total_services = len(status_data["services"])

        if healthy_services == total_services:
            status_data["overall_health"] = "healthy"
        elif healthy_services > total_services * 0.7:
            status_data["overall_health"] = "degraded"
        else:
            status_data["overall_health"] = "unhealthy"

        return {
            "status": "success",
            "data": status_data
        }

    except Exception as e:
        logger.error("Failed to get system status", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get system status")


@router.get("/metrics")
async def get_comprehensive_metrics(request: Request):
    """Get detailed system metrics"""
    try:
        metrics_data = {
            "timestamp": monitoring_system.get_comprehensive_status().get('timestamp'),
            "system_metrics": monitoring_system.get_comprehensive_status(),
            "component_metrics": {}
        }

        # Get stream processor metrics if available
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            metrics_data["component_metrics"]["stream_processor"] = stream_processor.get_stats()

        # Buffer manager metrics
        metrics_data["component_metrics"]["buffer_manager"] = buffer_manager.get_stats()

        # Task queue metrics
        metrics_data["component_metrics"]["task_queue"] = task_queue.get_stats()

        # Backpressure controller metrics
        metrics_data["component_metrics"]["backpressure_controller"] = backpressure_controller.get_stats()

        # Database optimizer metrics
        if database_optimizer:
            metrics_data["component_metrics"]["database_optimizer"] = database_optimizer.get_stats()

        # WebSocket metrics
        metrics_data["component_metrics"]["websocket_server"] = websocket_server.get_server_stats()

        # Exchange metrics
        metrics_data["component_metrics"]["exchanges"] = await exchange_manager.get_all_exchanges_status()

        return {
            "status": "success",
            "data": metrics_data
        }

    except Exception as e:
        logger.error("Failed to get comprehensive metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get metrics")


@router.post("/emergency")
async def emergency_action(request: Request, action_request: EmergencyActionRequest):
    """Perform emergency actions"""
    try:
        action = action_request.action.lower()
        result = {"action": action, "status": "completed"}

        if action == "throttle":
            # Apply emergency throttling through backpressure controller
            throttle_percent = action_request.throttle_percent or 90

            from ...stream_processing.backpressure_controller import ThrottleLevel
            if throttle_percent >= 90:
                backpressure_controller.throttle_level = ThrottleLevel.SEVERE
            elif throttle_percent >= 75:
                backpressure_controller.throttle_level = ThrottleLevel.HEAVY
            elif throttle_percent >= 50:
                backpressure_controller.throttle_level = ThrottleLevel.MODERATE
            else:
                backpressure_controller.throttle_level = ThrottleLevel.LIGHT

            result["throttle_percent"] = throttle_percent
            result["throttle_level"] = backpressure_controller.throttle_level.name

            logger.warning("Emergency throttle applied",
                           throttle_percent=throttle_percent,
                           reason=action_request.reason)

        elif action == "stop":
            # Emergency stop - maximum throttling
            from ...stream_processing.backpressure_controller import ThrottleLevel
            backpressure_controller.throttle_level = ThrottleLevel.SEVERE

            # Add all exchanges to throttled list
            for exchange in ["binance", "bybit", "whitebit"]:
                backpressure_controller.throttled_exchanges.add(exchange)

            result["message"] = "Emergency stop applied - all processing throttled"

            logger.critical("Emergency stop applied", reason=action_request.reason)

        elif action == "restart":
            # Reset throttling
            from ...stream_processing.backpressure_controller import ThrottleLevel
            backpressure_controller.throttle_level = ThrottleLevel.NONE
            backpressure_controller.throttled_exchanges.clear()

            # Reset circuit breakers
            backpressure_controller.circuit_breakers.clear()

            result["message"] = "System throttling reset"

            logger.info("Emergency restart completed", reason=action_request.reason)

        else:
            raise HTTPException(status_code=400, detail=f"Unknown emergency action: {action}")

        return {
            "status": "success",
            "data": result
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Emergency action failed", action=action_request.action, error=str(e))
        raise HTTPException(status_code=500, detail="Emergency action failed")


@router.get("/performance/recommendations")
async def get_performance_recommendations():
    """Get performance recommendations"""
    try:
        recommendations = monitoring_system.get_performance_recommendations()
        suggested_profile = monitoring_system.suggest_performance_profile()

        return {
            "status": "success",
            "data": {
                "recommendations": recommendations,
                "suggested_profile": suggested_profile,
                "current_profile": monitoring_system.current_profile,
                "system_health_score": monitoring_system.get_system_health_score()
            }
        }

    except Exception as e:
        logger.error("Failed to get performance recommendations", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get recommendations")


@router.post("/performance/config")
async def update_performance_config(request: Request, config_request: SystemConfigRequest):
    """Update system performance configuration"""
    try:
        updated_components = []

        # Update performance mode in monitoring
        if config_request.performance_mode:
            monitoring_system.current_profile = config_request.performance_mode
            updated_components.append("monitoring_profile")

        # Update buffer settings
        if config_request.buffer_settings:
            for data_type_name, settings in config_request.buffer_settings.items():
                from ...stream_processing.buffer_manager import DataType
                data_type = getattr(DataType, data_type_name.upper(), None)
                if data_type and data_type in buffer_manager.buffer_configs:
                    config = buffer_manager.buffer_configs[data_type]
                    if 'max_size' in settings:
                        config.max_size = settings['max_size']
                    if 'max_age_ms' in settings:
                        config.max_age_ms = settings['max_age_ms']
                    updated_components.append(f"buffer_{data_type_name}")

        # Update worker settings
        if config_request.worker_settings:
            from ...stream_processing.task_queue import WorkerType
            for worker_type_name, settings in config_request.worker_settings.items():
                worker_type = getattr(WorkerType, worker_type_name.upper(), None)
                if worker_type and worker_type in task_queue.worker_configs:
                    task_queue.worker_configs[worker_type].update(settings)
                    updated_components.append(f"worker_{worker_type_name}")

        # Update stream processor config if available
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            if config_request.performance_mode:
                stream_processor.config.performance_mode = config_request.performance_mode

        logger.info("Performance configuration updated",
                    components=updated_components,
                    performance_mode=config_request.performance_mode)

        return {
            "status": "success",
            "data": {
                "updated_components": updated_components,
                "performance_mode": config_request.performance_mode,
                "message": "Configuration updated successfully"
            }
        }

    except Exception as e:
        logger.error("Failed to update performance config", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to update configuration")


@router.get("/database/stats")
async def get_database_stats():
    """Get database statistics"""
    try:
        # Get basic database stats from migrator
        table_stats = await migrator.get_table_stats()

        # Get database optimizer stats if available
        optimizer_stats = {}
        if database_optimizer:
            optimizer_stats = database_optimizer.get_stats()

        return {
            "status": "success",
            "data": {
                "tables": table_stats,
                "total_size_mb": sum(table.get("size_mb", 0) for table in table_stats),
                "total_rows": sum(table.get("rows", 0) for table in table_stats),
                "optimizer_stats": optimizer_stats
            }
        }

    except Exception as e:
        logger.error("Failed to get database stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get database stats")


@router.post("/database/optimize")
async def optimize_database():
    """Optimize database performance"""
    try:
        # Run table optimization
        success = await migrator.optimize_tables()

        result = {
            "table_optimization": "completed" if success else "failed"
        }

        # Flush database optimizer buffers if available
        if database_optimizer:
            await database_optimizer._flush_all_batches()
            result["buffer_flush"] = "completed"

        return {
            "status": "success" if success else "partial",
            "data": result
        }

    except Exception as e:
        logger.error("Failed to optimize database", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to optimize database")


@router.get("/alerts")
async def get_active_alerts():
    """Get active system alerts"""
    try:
        active_alerts = monitoring_system.get_active_alerts()

        return {
            "status": "success",
            "data": {
                "active_alerts": active_alerts,
                "alert_count": len(active_alerts)
            }
        }

    except Exception as e:
        logger.error("Failed to get active alerts", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get alerts")


@router.get("/health/detailed")
async def detailed_health_check(request: Request):
    """Detailed system health check"""
    try:
        health_data = {
            "timestamp": monitoring_system.get_comprehensive_status().get('timestamp'),
            "overall": "healthy",
            "components": {},
            "issues": [],
            "recommendations": []
        }

        issues = []
        unhealthy_components = []

        # Check stream processor
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            stream_health = await stream_processor.health_check()
            health_data["components"]["stream_processor"] = stream_health

            if stream_health["overall"] == "unhealthy":
                unhealthy_components.extend(stream_health.get("unhealthy_components", []))
                issues.append("Stream processor has unhealthy components")

        # Check monitoring system
        monitoring_status = monitoring_system.get_comprehensive_status()
        health_score = monitoring_status.get('health_score', 0)
        health_data["components"]["monitoring"] = {
            "healthy": health_score > 50,
            "health_score": health_score,
            "status": monitoring_status.get('health_status', 'unknown')
        }

        if health_score < 50:
            issues.append(f"Low system health score: {health_score}")

        # Check exchanges
        exchange_status = await exchange_manager.get_all_exchanges_status()
        total_exchanges = exchange_status.get('total_exchanges', 0)
        health_data["components"]["exchanges"] = {
            "healthy": total_exchanges > 0,
            "total_exchanges": total_exchanges
        }

        if total_exchanges == 0:
            issues.append("No exchanges connected")

        # Check database
        try:
            db_health = True
            if database_optimizer:
                db_health = await database_optimizer.health_check()
            health_data["components"]["database"] = {
                "healthy": db_health
            }
            if not db_health:
                issues.append("Database connection issues")
        except Exception:
            health_data["components"]["database"] = {"healthy": False}
            issues.append("Database health check failed")

        # Determine overall health
        if len(issues) == 0:
            health_data["overall"] = "healthy"
        elif len(issues) <= 2:
            health_data["overall"] = "degraded"
        else:
            health_data["overall"] = "unhealthy"

        health_data["issues"] = issues
        health_data["recommendations"] = monitoring_system.get_performance_recommendations()

        return {
            "status": "success",
            "data": health_data
        }

    except Exception as e:
        logger.error("Detailed health check failed", error=str(e))
        raise HTTPException(status_code=500, detail="Health check failed")


@router.get("/config")
async def get_system_config(request: Request):
    """Get current system configuration"""
    try:
        from ...config import settings

        config_data = {
            "application": {
                "debug": settings.debug,
                "host": settings.host,
                "port": settings.port,
                "websocket_port": settings.websocket_port
            },
            "database": {
                "url": "***masked***",  # Don't expose full URL
                "pool_size": getattr(settings, 'mysql_pool_size', 10),
                "max_overflow": getattr(settings, 'mysql_max_overflow', 20)
            },
            "cache": {
                "redis_host": settings.redis_host,
                "redis_port": settings.redis_port,
                "ttl_ticker": settings.cache_ttl_ticker,
                "ttl_orderbook": settings.cache_ttl_orderbook,
                "ttl_klines": settings.cache_ttl_klines
            },
            "exchanges": {
                "rate_limits": settings.exchange_rate_limits,
                "supported": exchange_manager.get_supported_exchanges()
            },
            "performance": {
                "current_profile": monitoring_system.current_profile,
                "suggested_profile": monitoring_system.suggest_performance_profile()
            }
        }

        # Add stream processor config if available
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            config_data["stream_processor"] = {
                "performance_mode": stream_processor.config.performance_mode,
                "buffer_settings": stream_processor.config.buffer_settings,
                "worker_settings": stream_processor.config.worker_settings
            }

        return {
            "status": "success",
            "data": config_data
        }

    except Exception as e:
        logger.error("Failed to get system config", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get system config")


@router.post("/system/restart-component/{component}")
async def restart_component(component: str, request: Request):
    """Restart specific system component"""
    try:
        result = {"component": component, "status": "unknown"}

        if component == "buffer_manager":
            # Flush all buffers and restart
            await buffer_manager._flush_all_buffers(force=True)
            result["status"] = "flushed_and_running"
            result["message"] = "Buffer manager flushed"

        elif component == "backpressure_controller":
            # Reset throttling and circuit breakers
            from ...stream_processing.backpressure_controller import ThrottleLevel
            backpressure_controller.throttle_level = ThrottleLevel.NONE
            backpressure_controller.throttled_exchanges.clear()
            backpressure_controller.circuit_breakers.clear()
            result["status"] = "reset"
            result["message"] = "Backpressure controller reset"

        elif component == "websocket_server":
            # Get current stats (can't actually restart without disconnecting clients)
            stats = websocket_server.get_server_stats()
            result["status"] = "running"
            result["message"] = f"WebSocket server running with {stats['clients_connected']} clients"

        elif component == "database_optimizer":
            if database_optimizer:
                await database_optimizer._flush_all_batches()
                result["status"] = "flushed"
                result["message"] = "Database optimizer buffers flushed"
            else:
                result["status"] = "not_available"
                result["message"] = "Database optimizer not initialized"

        else:
            raise HTTPException(status_code=400, detail=f"Unknown component: {component}")

        logger.info("Component restart completed", component=component, status=result["status"])

        return {
            "status": "success",
            "data": result
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to restart component", component=component, error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to restart {component}")


@router.get("/logs/recent")
async def get_recent_logs(
        level: str = Query("INFO", regex="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$"),
        limit: int = Query(100, ge=1, le=1000)
):
    """Get recent log entries (placeholder - would integrate with actual logging system)"""
    try:
        # This is a placeholder implementation
        # In production, you would integrate with your actual logging aggregation system

        return {
            "status": "success",
            "message": "Log retrieval requires external log aggregation system integration",
            "suggestion": "Use structured logging output or external tools like ELK stack",
            "available_endpoints": [
                "/api/v1/admin/status - System status",
                "/api/v1/admin/metrics - System metrics",
                "/api/v1/admin/health/detailed - Detailed health check"
            ]
        }

    except Exception as e:
        logger.error("Failed to get logs", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get logs")

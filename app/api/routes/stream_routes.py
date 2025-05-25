import asyncio

from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import structlog

from ...services.websocket_server import websocket_server
from ...stream_processing.monitoring_system import monitoring_system
from ...stream_processing.backpressure_controller import backpressure_controller
from ...stream_processing.buffer_manager import buffer_manager
from ...stream_processing.task_queue import task_queue

router = APIRouter()
logger = structlog.get_logger().bind(component="stream_api")


class StreamMessageRequest(BaseModel):
    exchange: str
    symbol: str
    message_type: str  # ticker, orderbook, klines, trades
    data: Dict[str, Any]
    timeframe: Optional[str] = None


class StreamSubscriptionRequest(BaseModel):
    exchange: str
    symbol: str
    stream_type: str
    timeframe: Optional[str] = None


class ThrottleRequest(BaseModel):
    exchange: Optional[str] = None  # If None, applies globally
    throttle_percent: int = 50
    duration_seconds: Optional[int] = None


@router.post("/process")
async def process_stream_message(request: Request, message_request: StreamMessageRequest):
    """Process WebSocket message through stream processor"""
    try:
        if not hasattr(request.app.state, 'stream_processor'):
            raise HTTPException(status_code=503, detail="Stream processor not available")

        stream_processor = request.app.state.stream_processor

        # Process message through stream processor
        success = await stream_processor.process_message(
            exchange=message_request.exchange,
            symbol=message_request.symbol,
            message_type=message_request.message_type,
            data=message_request.data
        )

        # Also route to WebSocket clients if successful
        if success:
            await websocket_server.handle_stream_processor_data(
                exchange=message_request.exchange,
                symbol=message_request.symbol,
                message_type=message_request.message_type,
                data=message_request.data
            )

        return {
            "status": "success" if success else "throttled",
            "processed": success,
            "exchange": message_request.exchange,
            "symbol": message_request.symbol,
            "message_type": message_request.message_type
        }

    except Exception as e:
        logger.error("Failed to process stream message",
                     exchange=message_request.exchange,
                     symbol=message_request.symbol,
                     message_type=message_request.message_type,
                     error=str(e))
        raise HTTPException(status_code=500, detail="Failed to process stream message")


@router.get("/stats")
async def get_stream_stats(request: Request):
    """Get detailed stream processing statistics"""
    try:
        stats_data = {
            "websocket_server": websocket_server.get_server_stats(),
            "buffer_manager": buffer_manager.get_stats(),
            "task_queue": task_queue.get_stats(),
            "backpressure_controller": backpressure_controller.get_stats(),
            "monitoring": monitoring_system.get_comprehensive_status()
        }

        # Add stream processor stats if available
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            stats_data["stream_processor"] = stream_processor.get_stats()

        return {
            "status": "success",
            "data": stats_data
        }

    except Exception as e:
        logger.error("Failed to get stream stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get stream stats")


@router.get("/health")
async def get_stream_health(request: Request):
    """Get stream processing health status"""
    try:
        health_data = {
            "overall": "unknown",
            "components": {},
            "issues": []
        }

        issues = []

        # Check stream processor
        if hasattr(request.app.state, 'stream_processor'):
            stream_processor = request.app.state.stream_processor
            stream_health = await stream_processor.health_check()
            health_data["components"]["stream_processor"] = stream_health

            if stream_health["overall"] != "healthy":
                issues.append(f"Stream processor: {stream_health['overall']}")
        else:
            health_data["components"]["stream_processor"] = {"healthy": False, "status": "not_initialized"}
            issues.append("Stream processor not initialized")

        # Check WebSocket server
        ws_stats = websocket_server.get_server_stats()
        health_data["components"]["websocket_server"] = {
            "healthy": ws_stats["running"],
            "clients_connected": ws_stats["clients_connected"],
            "status": "running" if ws_stats["running"] else "stopped"
        }

        if not ws_stats["running"]:
            issues.append("WebSocket server not running")

        # Check buffer manager
        buffer_stats = buffer_manager.get_stats()
        buffer_healthy = buffer_stats["running"] and buffer_stats["flush_errors"] == 0
        health_data["components"]["buffer_manager"] = {
            "healthy": buffer_healthy,
            "total_buffers": buffer_stats["total_buffers"],
            "flush_errors": buffer_stats["flush_errors"],
            "status": "running" if buffer_stats["running"] else "stopped"
        }

        if not buffer_healthy:
            issues.append("Buffer manager issues detected")

        # Check task queue
        queue_stats = task_queue.get_stats()
        queue_healthy = queue_stats["running"]
        health_data["components"]["task_queue"] = {
            "healthy": queue_healthy,
            "total_workers": sum(len(workers) for workers in
                                 queue_stats["worker_stats"].values()) if "worker_stats" in queue_stats else 0,
            "status": "running" if queue_stats["running"] else "stopped"
        }

        if not queue_healthy:
            issues.append("Task queue not running")

        # Check backpressure controller
        bp_stats = backpressure_controller.get_stats()
        bp_healthy = bp_stats["running"] and bp_stats["system_state"] != "critical"
        health_data["components"]["backpressure_controller"] = {
            "healthy": bp_healthy,
            "system_state": bp_stats["system_state"],
            "throttle_level": bp_stats["throttle_level"],
            "status": "running" if bp_stats["running"] else "stopped"
        }

        if not bp_healthy:
            issues.append(f"System under pressure: {bp_stats['system_state']}")

        # Determine overall health
        if len(issues) == 0:
            health_data["overall"] = "healthy"
        elif len(issues) <= 2:
            health_data["overall"] = "degraded"
        else:
            health_data["overall"] = "unhealthy"

        health_data["issues"] = issues

        return {
            "status": "success",
            "data": health_data
        }

    except Exception as e:
        logger.error("Failed to get stream health", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get stream health")


@router.post("/throttle")
async def apply_throttle(throttle_request: ThrottleRequest):
    """Apply throttling to stream processing"""
    try:
        from ...stream_processing.backpressure_controller import ThrottleLevel

        # Determine throttle level
        throttle_percent = throttle_request.throttle_percent
        if throttle_percent >= 90:
            throttle_level = ThrottleLevel.SEVERE
        elif throttle_percent >= 75:
            throttle_level = ThrottleLevel.HEAVY
        elif throttle_percent >= 50:
            throttle_level = ThrottleLevel.MODERATE
        elif throttle_percent >= 25:
            throttle_level = ThrottleLevel.LIGHT
        else:
            throttle_level = ThrottleLevel.NONE

        if throttle_request.exchange:
            # Apply to specific exchange
            if throttle_percent > 0:
                backpressure_controller.throttled_exchanges.add(throttle_request.exchange)
            else:
                backpressure_controller.throttled_exchanges.discard(throttle_request.exchange)

            result = {
                "exchange": throttle_request.exchange,
                "throttle_percent": throttle_percent,
                "applied": "exchange_specific"
            }
        else:
            # Apply globally
            backpressure_controller.throttle_level = throttle_level
            result = {
                "throttle_percent": throttle_percent,
                "throttle_level": throttle_level.name,
                "applied": "global"
            }

        logger.info("Throttle applied",
                    exchange=throttle_request.exchange,
                    throttle_percent=throttle_percent,
                    throttle_level=throttle_level.name)

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        logger.error("Failed to apply throttle", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to apply throttle")


@router.delete("/throttle")
async def remove_throttle(exchange: Optional[str] = Query(None)):
    """Remove throttling"""
    try:
        from ...stream_processing.backpressure_controller import ThrottleLevel

        if exchange:
            # Remove throttling from specific exchange
            backpressure_controller.throttled_exchanges.discard(exchange)
            result = {
                "exchange": exchange,
                "throttle_removed": True
            }
        else:
            # Remove global throttling
            backpressure_controller.throttle_level = ThrottleLevel.NONE
            backpressure_controller.throttled_exchanges.clear()
            result = {
                "global_throttle_removed": True,
                "all_exchange_throttles_removed": True
            }

        logger.info("Throttle removed", exchange=exchange)

        return {
            "status": "success",
            "data": result
        }

    except Exception as e:
        logger.error("Failed to remove throttle", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to remove throttle")


@router.get("/buffers")
async def get_buffer_stats():
    """Get detailed buffer statistics"""
    try:
        buffer_stats = buffer_manager.get_stats()

        # Add additional analysis
        buffer_analysis = {
            "memory_pressure": buffer_manager.get_memory_pressure(),
            "buffer_efficiency": {
                "flush_rate": buffer_stats["total_items_flushed"] / max(buffer_stats["total_items_buffered"], 1),
                "error_rate": buffer_stats["flush_errors"] / max(buffer_stats["total_items_flushed"], 1)
            }
        }

        return {
            "status": "success",
            "data": {
                "stats": buffer_stats,
                "analysis": buffer_analysis
            }
        }

    except Exception as e:
        logger.error("Failed to get buffer stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get buffer stats")


@router.post("/buffers/flush")
async def flush_buffers(force: bool = Query(False)):
    """Manually flush all buffers"""
    try:
        await buffer_manager._flush_all_buffers(force=force)

        return {
            "status": "success",
            "data": {
                "message": "All buffers flushed",
                "force": force
            }
        }

    except Exception as e:
        logger.error("Failed to flush buffers", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to flush buffers")


@router.get("/workers")
async def get_worker_stats():
    """Get worker statistics and management info"""
    try:
        queue_stats = task_queue.get_stats()

        # Add worker analysis
        worker_analysis = {
            "total_workers": sum(len(workers) for workers in queue_stats.get("worker_stats", {}).values()),
            "worker_efficiency": {},
            "scaling_recommendations": []
        }

        # Calculate efficiency for each worker type
        for worker_type, workers in queue_stats.get("worker_stats", {}).items():
            if workers:
                total_processed = sum(w.get("tasks_processed", 0) for w in workers.values())
                total_failed = sum(w.get("tasks_failed", 0) for w in workers.values())
                success_rate = total_processed / max(total_processed + total_failed, 1) * 100

                worker_analysis["worker_efficiency"][worker_type] = {
                    "success_rate": success_rate,
                    "total_processed": total_processed,
                    "total_failed": total_failed
                }

        # Add scaling recommendations based on queue sizes
        for worker_type, queue_info in queue_stats.get("queue_stats", {}).items():
            queue_size = queue_info.get("queue_size", 0)
            worker_count = queue_info.get("worker_count", 0)

            if queue_size > 100 and worker_count < queue_info.get("config", {}).get("max", 10):
                worker_analysis["scaling_recommendations"].append({
                    "worker_type": worker_type,
                    "recommendation": "scale_up",
                    "reason": f"High queue size ({queue_size}) with available capacity"
                })
            elif queue_size < 10 and worker_count > queue_info.get("config", {}).get("min", 1):
                worker_analysis["scaling_recommendations"].append({
                    "worker_type": worker_type,
                    "recommendation": "scale_down",
                    "reason": f"Low queue size ({queue_size}) with excess workers"
                })

        return {
            "status": "success",
            "data": {
                "stats": queue_stats,
                "analysis": worker_analysis
            }
        }

    except Exception as e:
        logger.error("Failed to get worker stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get worker stats")


@router.get("/websocket/clients")
async def get_websocket_clients():
    """Get WebSocket client information"""
    try:
        ws_stats = websocket_server.get_server_stats()

        # Add client analysis
        client_analysis = {
            "subscription_distribution": ws_stats.get("subscription_breakdown", {}),
            "avg_subscriptions_per_client": (
                    ws_stats["total_subscriptions"] / max(ws_stats["clients_connected"], 1)
            ),
            "most_popular_streams": []
        }

        # Analyze subscription patterns
        breakdown = ws_stats.get("subscription_breakdown", {})
        if breakdown:
            sorted_streams = sorted(breakdown.items(), key=lambda x: x[1], reverse=True)
            client_analysis["most_popular_streams"] = sorted_streams[:5]

        return {
            "status": "success",
            "data": {
                "stats": ws_stats,
                "analysis": client_analysis
            }
        }

    except Exception as e:
        logger.error("Failed to get WebSocket client info", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get client info")


@router.post("/websocket/broadcast")
async def broadcast_test_message(message: Dict[str, Any]):
    """Broadcast test message to all WebSocket clients (for testing)"""
    try:
        # Prepare test message
        test_message = {
            "type": "test_broadcast",
            "message": message,
            "timestamp": monitoring_system.get_comprehensive_status().get('timestamp')
        }

        # Send to all clients
        tasks = []
        for client_id in websocket_server.clients:
            tasks.append(websocket_server._send_to_client(client_id, test_message))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        return {
            "status": "success",
            "data": {
                "message": "Test broadcast sent",
                "clients_count": len(websocket_server.clients),
                "broadcast_data": test_message
            }
        }

    except Exception as e:
        logger.error("Failed to broadcast test message", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to broadcast message")


@router.get("/performance")
async def get_performance_metrics():
    """Get comprehensive performance metrics"""
    try:
        # Get metrics from monitoring system
        monitoring_status = monitoring_system.get_comprehensive_status()

        # Get component-specific performance data
        performance_data = {
            "system_health_score": monitoring_status.get('health_score', 0),
            "current_profile": monitoring_status.get('current_profile', 'unknown'),
            "suggested_profile": monitoring_status.get('suggested_profile', 'unknown'),
            "key_metrics": monitoring_status.get('key_metrics', {}),
            "recommendations": monitoring_status.get('recommendations', []),
            "component_performance": {}
        }

        # Add buffer performance
        buffer_stats = buffer_manager.get_stats()
        performance_data["component_performance"]["buffers"] = {
            "efficiency": buffer_stats["total_items_flushed"] / max(buffer_stats["total_items_buffered"], 1),
            "error_rate": buffer_stats["flush_errors"] / max(buffer_stats["total_items_flushed"], 1),
            "memory_usage_mb": buffer_stats["memory_usage_mb"]
        }

        # Add task queue performance
        queue_stats = task_queue.get_stats()
        performance_data["component_performance"]["task_queue"] = {
            "completion_rate": queue_stats["total_tasks_completed"] / max(queue_stats["total_tasks_queued"], 1),
            "memory_usage_mb": queue_stats["memory_usage_mb"]
        }

        # Add backpressure performance
        bp_stats = backpressure_controller.get_stats()
        performance_data["component_performance"]["backpressure"] = {
            "system_state": bp_stats["system_state"],
            "throttle_level": bp_stats["throttle_level"],
            "throttled_exchanges_count": len(bp_stats["throttled_exchanges"])
        }

        return {
            "status": "success",
            "data": performance_data
        }

    except Exception as e:
        logger.error("Failed to get performance metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get performance metrics")

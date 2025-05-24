import time
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import structlog
from datetime import datetime, timedelta

from ...services.historical_data_service import historical_data_service, TaskPriority
from app.utils.rate_limiter import rate_limiter
from ...services.request_scheduler import request_scheduler


class OptimizedHistoricalRequest(BaseModel):
    exchange_slug: str
    symbol: str
    timeframe: str
    priority: str = "medium"
    since: Optional[int] = None
    limit: int = 1000


class BulkOptimizedImportRequest(BaseModel):
    exchange_slug: str
    symbols: List[str]
    timeframes: List[str]
    days_back: int = 30
    priority: str = "medium"


class CapacityOptimizationRequest(BaseModel):
    exchange_slug: str
    target_requests: int
    time_window_minutes: int = 60


router = APIRouter()
logger = structlog.get_logger().bind(component="historical_api")


@router.post("/historical/optimized")
async def schedule_optimized_historical_fetch(request: OptimizedHistoricalRequest):
    """Schedule optimized historical data fetch"""
    try:
        # Validate priority
        priority_map = {
            "critical": TaskPriority.CRITICAL,
            "high": TaskPriority.HIGH,
            "medium": TaskPriority.MEDIUM,
            "low": TaskPriority.LOW,
            "background": TaskPriority.BACKGROUND
        }

        priority = priority_map.get(request.priority.lower(), TaskPriority.MEDIUM)

        # Get optimization recommendations
        capacity = rate_limiter.get_exchange_capacity(request.exchange_slug)
        strategy = rate_limiter.calculate_bulk_fetch_strategy(
            request.exchange_slug, request.symbol, request.timeframe,
            (datetime.now() - datetime.fromtimestamp((request.since or 0) / 1000)).days
        )

        # Schedule optimized task
        task_id = await historical_data_service.schedule_optimized_task(
            exchange_slug=request.exchange_slug,
            symbol=request.symbol,
            timeframe=request.timeframe,
            priority=priority,
            since=request.since,
            limit=request.limit
        )

        return {
            "status": "success",
            "message": "Optimized historical data fetch scheduled",
            "task_id": task_id,
            "priority": request.priority,
            "optimization_info": {
                "estimated_requests": strategy["batches_needed"],
                "estimated_time_seconds": strategy["estimated_time_seconds"],
                "recommended_batch_size": strategy["batch_size"],
                "can_start_immediately": strategy["can_start_immediately"],
                "current_capacity": capacity
            }
        }

    except Exception as e:
        logger.error("Failed to schedule optimized historical fetch", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to schedule optimized fetch")


@router.post("/historical/bulk-optimized")
async def bulk_optimized_historical_import(request: BulkOptimizedImportRequest):
    """Schedule bulk optimized historical data import"""
    try:
        # Pre-flight capacity check
        total_capacity_needed = 0
        capacity_analysis = {}

        for symbol in request.symbols:
            for timeframe in request.timeframes:
                strategy = rate_limiter.calculate_bulk_fetch_strategy(
                    request.exchange_slug, symbol, timeframe, request.days_back
                )
                total_capacity_needed += strategy["batches_needed"]

        capacity_analysis = {
            "total_requests_needed": total_capacity_needed,
            "current_capacity": rate_limiter.get_exchange_capacity(request.exchange_slug),
            "estimated_completion_time": total_capacity_needed * rate_limiter.get_optimal_delay(
                request.exchange_slug, rate_limiter.RequestType.HISTORICAL_BULK
            )
        }

        # Schedule bulk import with optimization
        task_ids = await historical_data_service.bulk_import_with_optimization(
            exchange_slug=request.exchange_slug,
            symbols=request.symbols,
            timeframes=request.timeframes,
            days_back=request.days_back
        )

        return {
            "status": "success",
            "message": "Bulk optimized import scheduled",
            "task_ids": task_ids,
            "total_tasks": len(task_ids),
            "symbols_count": len(request.symbols),
            "timeframes_count": len(request.timeframes),
            "capacity_analysis": capacity_analysis
        }

    except Exception as e:
        logger.error("Failed to schedule bulk optimized import", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to schedule bulk optimized import")


@router.get("/historical/optimization-stats")
async def get_optimization_stats():
    """Get historical data optimization statistics"""
    try:
        stats = await historical_data_service.get_optimization_stats()

        return {
            "status": "success",
            "data": stats
        }

    except Exception as e:
        logger.error("Failed to get optimization stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get optimization stats")


@router.get("/historical/capacity/{exchange}")
async def get_exchange_capacity(exchange: str):
    """Get current exchange capacity and optimization recommendations"""
    try:
        capacity = rate_limiter.get_exchange_capacity(exchange)

        # Get queue stats
        queue_stats = await request_scheduler.get_queue_stats()
        exchange_queue = queue_stats.get(exchange, {})

        # Calculate recommendations
        recommendations = []

        if capacity.get("requests_remaining", 0) < 50:
            recommendations.append({
                "type": "capacity_warning",
                "message": "Low API capacity remaining",
                "action": "Consider delaying non-critical requests"
            })

        if capacity.get("consecutive_rate_limits", 0) > 2:
            recommendations.append({
                "type": "rate_limit_warning",
                "message": "Multiple recent rate limit hits detected",
                "action": "Increase delays between requests"
            })

        if exchange_queue.get("total", 0) > 100:
            recommendations.append({
                "type": "queue_warning",
                "message": "High number of queued requests",
                "action": "Consider distributing requests over longer time windows"
            })

        return {
            "status": "success",
            "exchange": exchange,
            "data": {
                "capacity": capacity,
                "queue_status": exchange_queue,
                "recommendations": recommendations,
                "optimal_timeframes": rate_limiter.suggest_optimal_timeframes(
                    exchange, ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
                )
            }
        }

    except Exception as e:
        logger.error("Failed to get exchange capacity", exchange=exchange, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get exchange capacity")


@router.post("/historical/optimize-capacity")
async def optimize_exchange_capacity(request: CapacityOptimizationRequest):
    """Optimize exchange capacity allocation"""
    try:
        # Analyze current usage patterns
        current_capacity = rate_limiter.get_exchange_capacity(request.exchange_slug)

        # Calculate optimal request distribution
        optimal_delay = rate_limiter.get_optimal_delay(
            request.exchange_slug, rate_limiter.RequestType.OHLCV
        )

        max_requests_per_window = min(
            request.target_requests,
            int((request.time_window_minutes * 60) / optimal_delay)
        )

        # Optimize queue distribution
        await request_scheduler.optimize_queue_distribution(request.exchange_slug)

        optimization_result = {
            "exchange": request.exchange_slug,
            "target_requests": request.target_requests,
            "time_window_minutes": request.time_window_minutes,
            "max_achievable_requests": max_requests_per_window,
            "optimal_delay_seconds": optimal_delay,
            "current_capacity": current_capacity,
            "optimization_applied": True
        }

        return {
            "status": "success",
            "message": "Capacity optimization applied",
            "data": optimization_result
        }

    except Exception as e:
        logger.error("Failed to optimize capacity", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to optimize capacity")


@router.get("/historical/queue-stats")
async def get_queue_statistics():
    """Get detailed queue statistics"""
    try:
        queue_stats = await request_scheduler.get_queue_stats()
        optimizer_stats = rate_limiter.get_optimizer_stats()

        return {
            "status": "success",
            "data": {
                "queue_statistics": queue_stats,
                "rate_limit_statistics": optimizer_stats,
                "timestamp": datetime.now().isoformat()
            }
        }

    except Exception as e:
        logger.error("Failed to get queue statistics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get queue statistics")


@router.post("/historical/emergency-throttle/{exchange}")
async def emergency_throttle_exchange(exchange: str, throttle_minutes: int = Query(5, ge=1, le=60)):
    """Emergency throttle for exchange (admin use)"""
    try:
        # Apply emergency throttle by setting adaptive delay
        rate_limiter.adaptive_delays[exchange] = throttle_minutes * 2
        rate_limiter.consecutive_limits[exchange] = 5  # Trigger high backoff

        # Pause scheduler temporarily
        request_scheduler.pause_until[exchange] = time.time() + (throttle_minutes * 60)

        logger.warning("Emergency throttle applied",
                       exchange=exchange, throttle_minutes=throttle_minutes)

        return {
            "status": "success",
            "message": f"Emergency throttle applied to {exchange}",
            "throttle_duration_minutes": throttle_minutes,
            "expected_resume_time": (datetime.now() + timedelta(minutes=throttle_minutes)).isoformat()
        }

    except Exception as e:
        logger.error("Failed to apply emergency throttle", exchange=exchange, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to apply emergency throttle")


@router.delete("/historical/emergency-throttle/{exchange}")
async def remove_emergency_throttle(exchange: str):
    """Remove emergency throttle from exchange"""
    try:
        # Reset throttle settings
        if exchange in rate_limiter.adaptive_delays:
            rate_limiter.adaptive_delays[exchange] = 1.0

        if exchange in rate_limiter.consecutive_limits:
            rate_limiter.consecutive_limits[exchange] = 0

        # Remove scheduler pause
        if exchange in request_scheduler.pause_until:
            del request_scheduler.pause_until[exchange]

        logger.info("Emergency throttle removed", exchange=exchange)

        return {
            "status": "success",
            "message": f"Emergency throttle removed from {exchange}",
            "current_capacity": rate_limiter.get_exchange_capacity(exchange)
        }

    except Exception as e:
        logger.error("Failed to remove emergency throttle", exchange=exchange, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to remove emergency throttle")

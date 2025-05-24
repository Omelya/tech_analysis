from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import structlog

from ...services.stream_integration import stream_integration
from ...services.websocket_server import websocket_server

router = APIRouter()
logger = structlog.get_logger().bind(component="stream_api")


class StreamRequest(BaseModel):
    exchange: str
    symbol: str
    stream_type: str
    timeframe: Optional[str] = None


@router.post("/streams/start")
async def start_stream(request: StreamRequest):
    """Start a new data stream"""
    try:
        from ...services.stream_manager import stream_manager

        stream_key = await stream_manager.start_stream(
            request.exchange,
            request.symbol,
            request.stream_type,
            request.timeframe
        )

        return {
            "status": "success",
            "stream_key": stream_key,
            "message": "Stream started successfully"
        }

    except Exception as e:
        logger.error("Failed to start stream", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to start stream")


@router.delete("/streams/{stream_key}")
async def stop_stream(stream_key: str):
    """Stop a data stream"""
    try:
        from ...services.stream_manager import stream_manager

        await stream_manager.stop_stream(stream_key)

        return {
            "status": "success",
            "message": "Stream stopped successfully"
        }

    except Exception as e:
        logger.error("Failed to stop stream", stream_key=stream_key, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to stop stream")


@router.get("/streams/stats")
async def get_stream_stats():
    """Get streaming statistics"""
    try:
        integration_stats = stream_integration.get_integration_stats()
        websocket_stats = websocket_server.get_server_stats()

        return {
            "status": "success",
            "data": {
                "integration": integration_stats,
                "websocket_server": websocket_stats
            }
        }

    except Exception as e:
        logger.error("Failed to get stream stats", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get stream stats")

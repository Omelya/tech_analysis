from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel
import structlog

from ...exchanges.manager import exchange_manager
from ...exchanges.factory import ExchangeFactory


class ExchangeCredentials(BaseModel):
    api_key: str
    api_secret: str
    passphrase: Optional[str] = None
    testnet: bool = False


class SubscriptionRequest(BaseModel):
    exchange_id: str
    stream_type: str  # ticker, orderbook, klines
    symbol: str
    credentials: Optional[ExchangeCredentials] = None
    options: Optional[Dict[str, Any]] = None


router = APIRouter()
logger = structlog.get_logger().bind(component="exchange_api")


@router.get("/exchanges")
async def get_supported_exchanges():
    """Get list of supported exchanges"""
    try:
        exchanges = ExchangeFactory.get_supported_exchanges()
        return {
            "status": "success",
            "exchanges": exchanges,
            "count": len(exchanges)
        }
    except Exception as e:
        logger.error("Failed to get supported exchanges", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/info")
async def get_exchange_info(exchange_id: str):
    """Get exchange information including markets and timeframes"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        info = await exchange_manager.get_exchange_info(exchange_id)
        if not info:
            raise HTTPException(status_code=500, detail=f"Failed to get {exchange_id} info")

        return {
            "status": "success",
            "data": info
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get exchange info", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/status")
async def get_exchange_status(exchange_id: str):
    """Get exchange connection status"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        status = await ExchangeFactory.get_adapter_status(exchange_id)
        return {
            "status": "success",
            "data": status
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get exchange status", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/status")
async def get_all_exchanges_status():
    """Get status of all exchanges"""
    try:
        status = await exchange_manager.get_all_exchanges_status()
        return {
            "status": "success",
            "data": status
        }

    except Exception as e:
        logger.error("Failed to get all exchanges status", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/exchanges/{exchange_id}/verify")
async def verify_exchange_credentials(exchange_id: str, credentials: ExchangeCredentials):
    """Verify exchange API credentials"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        creds_dict = {
            "api_key": credentials.api_key,
            "api_secret": credentials.api_secret
        }

        if credentials.passphrase:
            creds_dict["passphrase"] = credentials.passphrase

        is_valid = await exchange_manager.verify_exchange_credentials(exchange_id, creds_dict)

        return {
            "status": "success",
            "data": {
                "exchange_id": exchange_id,
                "valid": is_valid,
                "testnet": credentials.testnet
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to verify credentials", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/ticker/{symbol}")
async def get_ticker(exchange_id: str, symbol: str):
    """Get ticker data for symbol"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        ticker = await exchange_manager.fetch_ticker(exchange_id, symbol)
        if not ticker:
            raise HTTPException(status_code=404, detail=f"Ticker data not found for {symbol}")

        return {
            "status": "success",
            "data": ticker
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get ticker", exchange=exchange_id, symbol=symbol, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/ohlcv/{symbol}")
async def get_ohlcv(
        exchange_id: str,
        symbol: str,
        timeframe: str = Query(..., description="Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)"),
        since: Optional[int] = Query(None, description="Start time in milliseconds"),
        limit: Optional[int] = Query(100, description="Number of candles", le=1000)
):
    """Get OHLCV data for symbol"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        ohlcv = await exchange_manager.fetch_ohlcv(exchange_id, symbol, timeframe, since, limit)
        if not ohlcv:
            raise HTTPException(status_code=404, detail=f"OHLCV data not found for {symbol}")

        formatted_data = []
        for candle in ohlcv:
            formatted_data.append({
                "timestamp": candle[0],
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5]
            })

        return {
            "status": "success",
            "data": {
                "exchange": exchange_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "count": len(formatted_data),
                "candles": formatted_data
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get OHLCV", exchange=exchange_id, symbol=symbol,
                     timeframe=timeframe, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/orderbook/{symbol}")
async def get_orderbook(
        exchange_id: str,
        symbol: str,
        limit: Optional[int] = Query(100, description="Order book depth", le=1000)
):
    """Get order book data for symbol"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        orderbook = await exchange_manager.fetch_order_book(exchange_id, symbol, limit)
        if not orderbook:
            raise HTTPException(status_code=404, detail=f"Order book data not found for {symbol}")

        return {
            "status": "success",
            "data": orderbook
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get order book", exchange=exchange_id, symbol=symbol, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/exchanges/{exchange_id}/subscribe")
async def subscribe_to_stream(exchange_id: str, request: SubscriptionRequest):
    """Subscribe to WebSocket data stream"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        if request.exchange_id != exchange_id:
            raise HTTPException(status_code=400, detail="Exchange ID mismatch")

        credentials = None
        if request.credentials:
            credentials = {
                "api_key": request.credentials.api_key,
                "api_secret": request.credentials.api_secret
            }
            if request.credentials.passphrase:
                credentials["passphrase"] = request.credentials.passphrase

        async def data_callback(data):
            logger.info("Received WebSocket data", data=data)

        subscription_id = await exchange_manager.subscribe_to_stream(
            exchange_id=exchange_id,
            stream_type=request.stream_type,
            symbol=request.symbol,
            callback=data_callback,
            credentials=credentials,
            **(request.options or {})
        )

        if not subscription_id:
            raise HTTPException(status_code=500, detail="Failed to subscribe to stream")

        return {
            "status": "success",
            "data": {
                "subscription_id": subscription_id,
                "exchange_id": exchange_id,
                "stream_type": request.stream_type,
                "symbol": request.symbol
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to subscribe to stream", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/exchanges/{exchange_id}/subscribe/{subscription_id}")
async def unsubscribe_from_stream(exchange_id: str, subscription_id: str):
    """Unsubscribe from WebSocket data stream"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        success = await exchange_manager.unsubscribe_from_stream(exchange_id, subscription_id)

        if not success:
            raise HTTPException(status_code=404, detail="Subscription not found or failed to unsubscribe")

        return {
            "status": "success",
            "data": {
                "subscription_id": subscription_id,
                "exchange_id": exchange_id,
                "unsubscribed": True
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to unsubscribe from stream", exchange=exchange_id,
                     subscription_id=subscription_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/timeframes")
async def get_exchange_timeframes(exchange_id: str):
    """Get supported timeframes for exchange"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        adapter = await exchange_manager.get_public_adapter(exchange_id)
        if not adapter:
            raise HTTPException(status_code=500, detail=f"Failed to connect to {exchange_id}")

        timeframes = await adapter.get_timeframes()

        return {
            "status": "success",
            "data": {
                "exchange_id": exchange_id,
                "timeframes": timeframes,
                "count": len(timeframes)
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get timeframes", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/exchanges/{exchange_id}/markets")
async def get_exchange_markets(exchange_id: str):
    """Get available markets for exchange"""
    try:
        if exchange_id not in ExchangeFactory.get_supported_exchanges():
            raise HTTPException(status_code=404, detail=f"Exchange {exchange_id} not supported")

        adapter = await exchange_manager.get_public_adapter(exchange_id)
        if not adapter:
            raise HTTPException(status_code=500, detail=f"Failed to connect to {exchange_id}")

        markets = await adapter.get_markets()

        return {
            "status": "success",
            "data": {
                "exchange_id": exchange_id,
                "markets": markets,
                "count": len(markets) if markets else 0
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get markets", exchange=exchange_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

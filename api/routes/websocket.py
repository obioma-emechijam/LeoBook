# websocket.py: WS /ws/live endpoint.
# Part of LeoBook API — Routes

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api.services.live_service import live_manager

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws/live")
async def live_scores_ws(websocket: WebSocket):
    """
    WebSocket endpoint for live score streaming.

    Connect to receive real-time live score updates.
    Updates are pushed every 30 seconds when data changes.
    """
    await live_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; client can send pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        live_manager.disconnect(websocket)
    except Exception:
        live_manager.disconnect(websocket)

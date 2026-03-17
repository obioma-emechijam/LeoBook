# live_service.py: WebSocket manager for live scores.
# Part of LeoBook API — Services

"""
WebSocket connection manager for live score streaming.
Pushes updates to all connected Flutter clients when live_scores changes.
"""

import asyncio
import logging
import json
from typing import List, Set
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class LiveConnectionManager:
    """Manages WebSocket connections for live score streaming."""

    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._polling_task = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self._connections.add(websocket)
        logger.info(f"[WS] Client connected. Total: {len(self._connections)}")

        # Send initial data
        try:
            data = self._fetch_live_scores()
            await websocket.send_json({"type": "initial", "data": data})
        except Exception as e:
            logger.warning(f"[WS] Failed to send initial data: {e}")

    def disconnect(self, websocket: WebSocket):
        self._connections.discard(websocket)
        logger.info(f"[WS] Client disconnected. Total: {len(self._connections)}")

    async def broadcast(self, data: dict):
        """Send data to all connected clients."""
        dead = set()
        for ws in self._connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self._connections -= dead

    def _fetch_live_scores(self) -> list:
        """Fetch current live scores from Supabase."""
        try:
            from api.services.prediction_service import _get_client
            client = _get_client()
            response = client.table("live_scores").select("*").execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"[WS] Failed to fetch live scores: {e}")
            return []

    async def start_polling(self, interval: int = 30):
        """Poll live_scores every N seconds and broadcast changes."""
        self._polling_task = asyncio.create_task(self._poll_loop(interval))

    async def _poll_loop(self, interval: int):
        last_hash = ""
        while True:
            try:
                data = self._fetch_live_scores()
                current_hash = json.dumps(data, sort_keys=True, default=str)
                if current_hash != last_hash and self._connections:
                    await self.broadcast({"type": "update", "data": data})
                    last_hash = current_hash
            except Exception as e:
                logger.warning(f"[WS] Polling error: {e}")
            await asyncio.sleep(interval)

    async def stop_polling(self):
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass


# Singleton
live_manager = LiveConnectionManager()

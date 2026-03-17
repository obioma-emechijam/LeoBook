# rate_limiter.py: Simple in-memory rate limiter.
# Part of LeoBook API — Middleware

"""
In-memory sliding-window rate limiter.
Swap to Redis-backed for multi-process deployments.
"""

import time
from collections import defaultdict
from fastapi import Request, HTTPException, status


class RateLimiter:
    """Simple per-IP rate limiter using sliding window."""

    def __init__(self, requests_per_minute: int = 60):
        self.rpm = requests_per_minute
        self._hits: dict = defaultdict(list)

    def _clean_old(self, key: str, now: float):
        cutoff = now - 60.0
        self._hits[key] = [t for t in self._hits[key] if t > cutoff]

    def check(self, request: Request):
        """Check rate limit. Raises 429 if exceeded."""
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        self._clean_old(client_ip, now)

        if len(self._hits[client_ip]) >= self.rpm:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded ({self.rpm}/min). Try again later.",
            )
        self._hits[client_ip].append(now)


# Singleton
rate_limiter = RateLimiter()

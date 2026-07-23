"""Fixed-rate spacing limiter aligned with sliding window APIs."""

from __future__ import annotations

import asyncio
import time


class FixedRateLimiter:
    """Rate limiter that enforces fixed spacing between requests.

    Instead of a Token Bucket (which can accumulate tokens and allow bursts),
    this limiter records the timestamp of each request and ensures that at
    least ``min_interval`` seconds elapse between consecutive requests.

    This is the correct algorithm for providers like NVIDIA Build API that
    enforce a sliding-window RPM limit with strict burst penalties.

    How it works::

        min_interval = 60 / requests_per_minute

        request 1 → t=0.000  (sent immediately)
        request 2 → t=0.000  (wait until t=1.714)
        request 3 → t=1.714  (wait until t=3.428)
        ...

    The limiter never allows two requests closer than ``min_interval``.
    """

    def __init__(self, requests_per_minute: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be > 0")
        self._min_interval: float = 60.0 / requests_per_minute
        self._last_request_time: float = 0.0  # epoch: allow immediate first request
        self._request_count: int = 0
        self._window_start: float = time.monotonic()
        self._window_count: int = 0

    @property
    def min_interval(self) -> float:
        """Minimum seconds between consecutive requests."""
        return self._min_interval

    async def acquire(self) -> None:
        """Block until it is safe to send the next request.

        Enforces exact spacing: if the previous request was sent less than
        ``min_interval`` seconds ago, this coroutine sleeps for the
        remaining time.
        """
        now = time.monotonic()
        elapsed = now - self._last_request_time

        if elapsed < self._min_interval:
            wait = self._min_interval - elapsed
            await asyncio.sleep(wait)

        self._last_request_time = time.monotonic()
        self._request_count += 1

        # Sliding window counter (for metrics / diagnostics)
        window_now = time.monotonic()
        if window_now - self._window_start >= 60.0:
            self._window_start = window_now
            self._window_count = 0
        self._window_count += 1

    @property
    def effective_rpm(self) -> int:
        """Requests sent in the current 60s window (diagnostic)."""
        now = time.monotonic()
        if now - self._window_start >= 60.0:
            return 0
        return self._window_count

    @property
    def total_requests(self) -> int:
        """Total requests that have passed through this limiter."""
        return self._request_count

    @property
    def seconds_until_next(self) -> float:
        """Seconds until the next request can be sent (0 if ready now)."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        remaining = self._min_interval - elapsed
        return max(0.0, remaining)

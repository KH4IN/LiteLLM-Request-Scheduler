"""Metrics collection and reporting."""

from __future__ import annotations

import time
from collections import deque


class Metrics:
    """Tracks proxy performance metrics.

    Uses sliding windows for RPM and bounded deques for recent samples
    so memory usage stays constant regardless of uptime.
    """

    MAX_SAMPLES: int = 1000

    def __init__(self) -> None:
        self._start_time: float = time.monotonic()
        self._processed: int = 0
        self._failed: int = 0
        self._queue_size: int = 0
        self._wait_times: deque[float] = deque(maxlen=self.MAX_SAMPLES)
        self._processing_times: deque[float] = deque(maxlen=self.MAX_SAMPLES)
        self._rpm_window: deque[float] = deque()

    # ------------------------------------------------------------------
    # Record methods
    # ------------------------------------------------------------------

    def increment_processed(self) -> None:
        """Record a successfully processed request."""
        self._processed += 1
        now = time.monotonic()
        self._rpm_window.append(now)
        self._cleanup_rpm_window(now)

    def increment_failed(self) -> None:
        """Record a failed request."""
        self._failed += 1

    def record_wait_time(self, seconds: float) -> None:
        """Record time a request spent waiting in queue."""
        self._wait_times.append(seconds)

    def record_processing_time(self, seconds: float) -> None:
        """Record time the proxy spent communicating with the backend."""
        self._processing_times.append(seconds)

    def set_queue_size(self, size: int) -> None:
        """Update the current queue depth."""
        self._queue_size = size

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _cleanup_rpm_window(self, now: float) -> None:
        """Purge entries older than 60 s from the RPM sliding window."""
        cutoff = now - 60.0
        while self._rpm_window and self._rpm_window[0] < cutoff:
            self._rpm_window.popleft()

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def rpm(self) -> int:
        """Current effective requests per minute (sliding window)."""
        now = time.monotonic()
        self._cleanup_rpm_window(now)
        return len(self._rpm_window)

    @property
    def processed_count(self) -> int:
        return self._processed

    @property
    def failed_count(self) -> int:
        return self._failed

    @property
    def queue_size(self) -> int:
        return self._queue_size

    @property
    def average_wait(self) -> float:
        """Average seconds a request waited in queue before processing."""
        if not self._wait_times:
            return 0.0
        return sum(self._wait_times) / len(self._wait_times)

    @property
    def average_processing(self) -> float:
        """Average seconds to forward a request and receive the response."""
        if not self._processing_times:
            return 0.0
        return sum(self._processing_times) / len(self._processing_times)

    @property
    def uptime(self) -> str:
        """Human-readable uptime string."""
        elapsed = int(time.monotonic() - self._start_time)
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m {seconds}s"

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Export all metrics as a JSON-serialisable dictionary."""
        return {
            "status": "ok",
            "uptime": self.uptime,
            "processed_requests": self._processed,
            "failed_requests": self._failed,
            "current_rpm": self.rpm,
            "average_wait_seconds": round(self.average_wait, 3),
            "average_processing_seconds": round(self.average_processing, 3),
            "queue_size": self._queue_size,
            "recent_samples": len(self._wait_times),
        }

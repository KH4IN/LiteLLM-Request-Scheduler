"""Single async worker that processes the FIFO request queue.

Retries, rate-limiting, and 429 diagnostics are all managed here so
that every HTTP attempt to the backend passes through the rate limiter.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

from config import AppConfig, RetryConfig
from limiter import FixedRateLimiter
from metrics import Metrics
from models import PendingRequest, StreamChunk
from proxy import LiteLLMProxy, RetryableResult, RETRYABLE_STATUS_CODES

logger = logging.getLogger(__name__)


class Worker:
    """Single async worker that dequeues requests one-by-one.

    Processing pipeline per request:
      1. Dequeue from the FIFO ``asyncio.Queue``.
      2. Loop: rate-limit → forward (single attempt) → check result.
         - On success → done.
         - On retryable failure → backoff (respecting Retry-After) → retry.
         - On non-retryable failure → pass error to client.
      3. Place the response into the request's own ``response_queue``.

    Only this worker communicates with the backend — all traffic is
    serialised through it.
    """

    def __init__(
        self,
        config: AppConfig,
        proxy: LiteLLMProxy,
        metrics: Metrics,
    ) -> None:
        maxsize = config.queue.max_size if config.queue.max_size > 0 else 0
        self._queue: asyncio.Queue[PendingRequest] = asyncio.Queue(maxsize=maxsize)
        self._limiter = FixedRateLimiter(
            requests_per_minute=config.rate_limit.requests_per_minute,
        )
        self._proxy = proxy
        self._metrics = metrics
        self._retry_config: RetryConfig = config.retry
        self._task: Optional[asyncio.Task[None]] = None
        self._running: bool = False
        self._processing: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def is_running(self) -> bool:
        """``True`` while the worker loop is active."""
        return self._running

    @property
    def is_processing(self) -> bool:
        """``True`` while the worker is actively handling a request."""
        return self._processing

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the background worker task."""
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="queue-worker")
        logger.info("Worker started")

    async def stop(self) -> None:
        """Cancel the background worker and wait for it to finish."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Worker stopped")

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    async def enqueue(self, request: PendingRequest) -> None:
        """Place *request* at the tail of the FIFO queue."""
        await self._queue.put(request)
        self._metrics.set_queue_size(self._queue.qsize())
        logger.info(
            f"[{request.id}] Enqueued | queue={self._queue.qsize()} "
            f"method={request.method} path={request.path} stream={request.is_stream}"
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _loop(self) -> None:
        """Core loop: dequeue → process."""
        while self._running:
            try:
                request = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            await self._process(request)

    # ------------------------------------------------------------------
    # Per-request processing (rate-limit + retry loop)
    # ------------------------------------------------------------------

    async def _process(self, request: PendingRequest) -> None:
        """Process a single request: rate-limit → forward → retry if needed."""
        self._processing = True
        try:
            request.processing_started = time.monotonic()
            wait_time = request.processing_started - request.enqueue_time
            self._metrics.record_wait_time(wait_time)
            self._metrics.set_queue_size(self._queue.qsize())

            logger.info(
                f"[{request.id}] Dequeued | wait={wait_time:.2f}s "
                f"queue={self._queue.qsize()}"
            )

            if request.cancelled.is_set():
                logger.info(f"[{request.id}] Cancelled before processing, skipping")
                return

            # ── Retry loop ──────────────────────────────────────────
            last_result: Optional[RetryableResult] = None

            for attempt in range(self._retry_config.attempts):
                if request.cancelled.is_set():
                    logger.info(f"[{request.id}] Cancelled before attempt {attempt + 1}")
                    return

                request.retries = attempt

                # Rate-limit BEFORE every attempt (including retries)
                await self._limiter.acquire()

                if request.cancelled.is_set():
                    logger.info(f"[{request.id}] Cancelled during rate-limit, skipping")
                    return

                send_time = time.monotonic()

                try:
                    await self._proxy.forward(request)

                    # Success — record and return
                    elapsed = time.monotonic() - send_time
                    self._metrics.record_processing_time(elapsed)
                    self._metrics.increment_processed()
                    logger.info(
                        f"[{request.id}] Completed | attempt={attempt + 1} "
                        f"processing={elapsed:.2f}s rpm={self._metrics.rpm}"
                    )
                    return

                except RetryableResult as result:
                    last_result = result
                    is_last = attempt >= self._retry_config.attempts - 1

                    # ── Structured 429 logging ──────────────────────
                    if result.status_code == 429:
                        self._log_429(request, result, attempt + 1, send_time)
                    else:
                        logger.warning(
                            f"[{request.id}] Retryable HTTP {result.status_code} "
                            f"on attempt {attempt + 1}/{self._retry_config.attempts} "
                            f"endpoint={request.path} "
                            f"origin={result.origin} body={result.body_text[:300]}"
                        )

                    if is_last:
                        break

                    # ── Backoff: max(config_backoff, Retry-After) ────
                    config_delay = self._get_delay(attempt)
                    retry_after = result.retry_after
                    if retry_after is not None and retry_after > 0:
                        delay = max(config_delay, retry_after)
                        logger.info(
                            f"[{request.id}] Retry-After={retry_after:.1f}s "
                            f"config_backoff={config_delay:.1f}s → waiting {delay:.1f}s"
                        )
                    else:
                        delay = config_delay
                        logger.info(
                            f"[{request.id}] Backoff {delay:.1f}s "
                            f"(attempt {attempt + 1}/{self._retry_config.attempts})"
                        )

                    await asyncio.sleep(delay)

            # ── All retries exhausted ───────────────────────────────
            self._emit_exhausted(request, last_result)

        except Exception as exc:
            logger.error(f"[{request.id}] Unexpected worker error: {exc}", exc_info=True)
            try:
                body = json.dumps({
                    "error": {"message": "Internal proxy error", "type": "proxy_error"}
                }).encode()
                await request.response_queue.put(
                    StreamChunk(
                        status_code=500,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        done=True,
                    )
                )
            except Exception:
                pass
            self._metrics.increment_failed()

        finally:
            self._processing = False
            self._queue.task_done()

    # ------------------------------------------------------------------
    # Retry delay calculation
    # ------------------------------------------------------------------

    def _get_delay(self, attempt: int) -> float:
        """Calculate back-off delay for the given attempt (0-indexed)."""
        if self._retry_config.exponential:
            delay = self._retry_config.initial_delay * (2**attempt)
        else:
            delay = self._retry_config.initial_delay
        return min(delay, self._retry_config.max_delay)

    # ------------------------------------------------------------------
    # 429 structured logging
    # ------------------------------------------------------------------

    def _log_429(
        self,
        request: PendingRequest,
        result: RetryableResult,
        attempt: int,
        send_time: float,
    ) -> None:
        """Log a 429 with full diagnostic context."""
        response_time = time.monotonic()
        retry_after_str = f"{result.retry_after:.1f}s" if result.retry_after else "not set"

        # Determine backend URL for origin hint
        backend_url = self._proxy._base_url  # noqa: WPS437 — intentional
        is_localhost = "localhost" in backend_url or "127.0.0.1" in backend_url

        origin_hint = "unknown"
        if is_localhost:
            origin_hint = "via-litellm (proxy→localhost, likely forwarded from NVIDIA)"
        else:
            origin_hint = f"direct ({backend_url})"

        logger.warning(
            f"[{request.id}] *** HTTP 429 *** | "
            f"attempt={attempt}/{self._retry_config.attempts} | "
            f"endpoint={request.method} {request.path} | "
            f"send_time={send_time:.3f} | "
            f"response_time={response_time:.3f} | "
            f"latency={response_time - send_time:.3f}s | "
            f"origin={result.origin} | "
            f"origin_hint={origin_hint} | "
            f"retry_after={retry_after_str} | "
            f"body={result.body_text[:500]} | "
            f"headers={json.dumps({k: v for k, v in result.headers.items() if k.lower().startswith(('x-ratelimit', 'retry', 'x-nvidia', 'x-'))}, default=str)[:500]}"
        )

    # ------------------------------------------------------------------
    # Retries exhausted
    # ------------------------------------------------------------------

    def _emit_exhausted(
        self,
        request: PendingRequest,
        last_result: Optional[RetryableResult],
    ) -> None:
        """Send final error to client and log exhaustion."""
        status = last_result.status_code if last_result else 0
        detail = last_result.body_text[:500] if last_result else "no response"

        logger.error(
            f"[{request.id}] RETRIES EXHAUSTED | "
            f"attempts={self._retry_config.attempts} | "
            f"endpoint={request.method} {request.path} | "
            f"last_status={status} | "
            f"origin={last_result.origin if last_result else 'unknown'} | "
            f"detail={detail}"
        )

        body = json.dumps({
            "error": {
                "message": "All retry attempts exhausted",
                "type": "proxy_error",
                "last_status": status,
                "detail": detail,
            }
        }).encode()
        try:
            request.response_queue.put_nowait(
                StreamChunk(
                    status_code=502,
                    data=body,
                    headers={"Content-Type": "application/json"},
                    done=True,
                )
            )
        except Exception:
            pass
        self._metrics.increment_failed()

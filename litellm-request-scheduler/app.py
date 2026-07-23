"""FastAPI application — LiteLLM Request Scheduler.

Continue sends requests to this proxy instead of directly to LiteLLM.
The proxy queues them, applies fixed-rate spacing, and forwards
them serially to the LiteLLM backend.  Every client gets its own
response — results are never mixed.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from config import AppConfig, load_config
from metrics import Metrics
from models import PendingRequest, StreamChunk
from proxy import LiteLLMProxy
from worker import Worker

logger = logging.getLogger("litellm_request_scheduler")

# ── Module-level references populated in lifespan ──────────────────────
_config: AppConfig
_proxy: LiteLLMProxy
_worker: Worker
_metrics: Metrics


# ── Helpers ───────────────────────────────────────────────────────────

def _detect_stream(body: bytes) -> bool:
    """Return ``True`` if the JSON body contains ``"stream": true``."""
    if not body:
        return False
    try:
        data = json.loads(body)
        return bool(data.get("stream", False))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False


# ── Lifespan ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _config, _proxy, _worker, _metrics

    _config = load_config()
    _metrics = Metrics()
    _proxy = LiteLLMProxy(_config)
    _worker = Worker(_config, _proxy, _metrics)

    await _proxy.start()
    await _worker.start()

    logger.info(
        f"LiteLLM Request Scheduler started | "
        f"listen={_config.proxy.host}:{_config.proxy.port} | "
        f"backend={_config.litellm.url} | "
        f"rpm={_config.rate_limit.requests_per_minute}"
    )
    yield
    await _worker.stop()
    await _proxy.stop()
    logger.info("LiteLLM Request Scheduler stopped")


# ── FastAPI app ───────────────────────────────────────────────────────

app = FastAPI(
    title="LiteLLM Request Scheduler",
    description="FIFO queue proxy with fixed-rate spacing for LiteLLM",
    lifespan=lifespan,
)


# ── Streaming response handler ────────────────────────────────────────

async def _handle_stream(pending: PendingRequest) -> StreamingResponse:
    """Wait for the first chunk then return a ``StreamingResponse``."""
    first = await pending.response_queue.get()
    pending.response_queue.task_done()

    # Error or single-shot response伪装成streaming请求
    if first.done:
        return Response(
            content=first.data or b"",
            status_code=first.status_code,
            headers=first.headers or {},
            media_type=(
                first.headers.get("content-type", "application/json")
                if first.headers
                else "application/json"
            ),
        )

    async def _stream_gen() -> AsyncGenerator[bytes, None]:
        try:
            if first.data:
                yield first.data
            while True:
                chunk = await pending.response_queue.get()
                pending.response_queue.task_done()
                if chunk.done:
                    break
                if chunk.data:
                    yield chunk.data
        except asyncio.CancelledError:
            pending.cancelled.set()
            raise

    resp_headers = _filter_resp_headers(first.headers or {})
    return StreamingResponse(
        _stream_gen(),
        status_code=first.status_code,
        headers=resp_headers,
        media_type="text/event-stream",
    )


# ── Non-streaming response handler ────────────────────────────────────

async def _handle_normal(pending: PendingRequest) -> Response:
    """Block until the worker finishes, then return the full response."""
    result = await pending.response_queue.get()
    pending.response_queue.task_done()
    return Response(
        content=result.data or b"",
        status_code=result.status_code,
        headers=result.headers or {},
    )


# ── Response header filter ────────────────────────────────────────────

_SKIP_RESP: frozenset[str] = frozenset({"transfer-encoding", "content-length", "connection"})


def _filter_resp_headers(headers: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in _SKIP_RESP}


# ── Catch-all proxy route ─────────────────────────────────────────────

@app.api_route(
    "/v1/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"],
)
async def proxy_endpoint(request: Request, path: str) -> Response:
    """Accept any ``/v1/*`` request, queue it, and return the backend response."""
    body = await request.body()
    is_stream = _detect_stream(body)

    pending = PendingRequest(
        method=request.method,
        path=f"/v1/{path}",
        headers=dict(request.headers),
        body=body,
        query_params=str(request.url.query) if request.url.query else "",
        is_stream=is_stream,
    )

    try:
        await _worker.enqueue(pending)
        if is_stream:
            return await _handle_stream(pending)
        return await _handle_normal(pending)
    except asyncio.CancelledError:
        pending.cancelled.set()
        raise


# ── Health ────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    """Quick liveness + queue depth check."""
    return {
        "status": "ok",
        "queue_size": _metrics.queue_size,
        "processing": _worker.is_processing,
        "processed_requests": _metrics.processed_count,
        "average_wait_seconds": _metrics.average_wait,
        "uptime": _metrics.uptime,
    }


# ── Metrics ───────────────────────────────────────────────────────────

@app.get("/metrics")
async def metrics_endpoint() -> dict:
    """Full performance metrics."""
    return _metrics.to_dict()


# ── Standalone entry point ────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    from logging_config import setup_logging

    cfg = load_config()
    setup_logging(cfg.logging.level)
    uvicorn.run(
        "app:app",
        host=cfg.proxy.host,
        port=cfg.proxy.port,
        log_level="info",
    )

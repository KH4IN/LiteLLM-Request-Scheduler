"""HTTP proxy client for forwarding requests to LiteLLM.

This module makes a **single attempt** per call.  Retries are managed by
the worker so that each attempt passes through the rate limiter.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import httpx

from config import AppConfig
from models import PendingRequest, StreamChunk

logger = logging.getLogger(__name__)

# Status codes that should trigger a retry
RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

# Headers we must NOT forward from the client to the backend
REQUEST_HEADERS_SKIP: frozenset[str] = frozenset({
    "host",
    "transfer-encoding",
    "connection",
    "content-length",
})

# Headers we must NOT forward from the backend to the client
RESPONSE_HEADERS_SKIP: frozenset[str] = frozenset({
    "transfer-encoding",
    "content-length",
    "connection",
})


class RetryableResult(Exception):
    """Result from a single forwarding attempt that may need a retry.

    When the backend returns a retryable status code (429, 5xx), the proxy
    raises this so the worker can inspect headers (Retry-After), body, and
    origin before deciding the next action.
    """

    __slots__ = (
        "status_code",
        "headers",
        "body_text",
        "body_json",
        "retry_after",
        "is_stream",
    )

    def __init__(
        self,
        status_code: int,
        headers: Optional[dict[str, str]] = None,
        body_text: str = "",
        body_json: Optional[Any] = None,
        retry_after: Optional[float] = None,
        is_stream: bool = False,
    ) -> None:
        super().__init__(f"Retryable HTTP {status_code}")
        self.status_code = status_code
        self.headers = headers or {}
        self.body_text = body_text
        self.body_json = body_json
        self.retry_after = retry_after
        self.is_stream = is_stream

    @property
    def origin(self) -> str:
        """Best guess at the origin of the 429: 'nvidia', 'litellm', or 'unknown'."""
        if self.body_json and isinstance(self.body_json, dict):
            body_str = json.dumps(self.body_json).lower()
        else:
            body_str = self.body_text.lower()

        # NVIDIA Build API typical 429 body patterns
        if "nvidia" in body_str or "nim" in body_str:
            return "nvidia"
        if "litellm" in body_str:
            return "litellm"
        # NVIDIA uses "integrate.api.nvidia.com" in error URLs
        if "integrate.api.nvidia" in body_str:
            return "nvidia"
        return "unknown"

    def __repr__(self) -> str:
        return (
            f"RetryableResult(status={self.status_code}, "
            f"retry_after={self.retry_after}, origin={self.origin})"
        )


class LiteLLMProxy:
    """HTTP client that forwards a single request to LiteLLM.

    Each call makes exactly one HTTP request.  Retries and rate-limiting
    are the caller's responsibility.
    """

    def __init__(self, config: AppConfig) -> None:
        self._base_url: str = config.litellm.url.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the underlying httpx client."""
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout=300.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        logger.info(f"LiteLLM proxy client started -> {self._base_url}")

    async def stop(self) -> None:
        """Close the underlying httpx client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Header helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _filter_request_headers(headers: dict[str, str]) -> dict[str, str]:
        return {k: v for k, v in headers.items() if k.lower() not in REQUEST_HEADERS_SKIP}

    @staticmethod
    def _filter_response_headers(headers: dict[str, str]) -> dict[str, str]:
        return {k: v for k, v in headers.items() if k.lower() not in RESPONSE_HEADERS_SKIP}

    # ------------------------------------------------------------------
    # URL builder
    # ------------------------------------------------------------------

    @staticmethod
    def _build_url(path: str, query_params: str) -> str:
        if query_params:
            return f"{path}?{query_params}"
        return path

    # ------------------------------------------------------------------
    # Single-attempt forward
    # ------------------------------------------------------------------

    async def forward(self, request: PendingRequest) -> None:
        """Forward *request* to LiteLLM in a single attempt.

        On success the response is placed into ``request.response_queue``.
        On retryable failure a :class:`RetryableResult` is raised so the
        worker can decide whether and when to retry.
        """
        assert self._client is not None, "Proxy client not started"
        url = self._build_url(request.path, request.query_params)
        headers = self._filter_request_headers(request.headers)

        try:
            if request.is_stream:
                await self._forward_stream(request, url, headers)
            else:
                await self._forward_normal(request, url, headers)
        except RetryableResult:
            raise  # propagated to worker for retry logic
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                httpx.RemoteProtocolError) as exc:
            raise RetryableResult(
                status_code=0,
                headers={},
                body_text=str(exc),
                is_stream=request.is_stream,
            ) from exc

    # ------------------------------------------------------------------
    # Non-streaming forward
    # ------------------------------------------------------------------

    async def _forward_normal(
        self, request: PendingRequest, url: str, headers: dict[str, str]
    ) -> None:
        assert self._client is not None
        response = await self._client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=request.body or None,
        )

        if response.status_code in RETRYABLE_STATUS_CODES:
            response.read()
            resp_headers = dict(response.headers)
            body_text = response.text[:2000]
            body_json = None
            try:
                body_json = response.json()
            except Exception:
                pass
            retry_after = _parse_retry_after(resp_headers)
            raise RetryableResult(
                status_code=response.status_code,
                headers=resp_headers,
                body_text=body_text,
                body_json=body_json,
                retry_after=retry_after,
                is_stream=False,
            )

        resp_headers = self._filter_response_headers(dict(response.headers))
        await request.response_queue.put(
            StreamChunk(
                status_code=response.status_code,
                headers=resp_headers,
                data=response.content,
                done=True,
            )
        )

    # ------------------------------------------------------------------
    # Streaming forward
    # ------------------------------------------------------------------

    async def _forward_stream(
        self, request: PendingRequest, url: str, headers: dict[str, str]
    ) -> None:
        assert self._client is not None
        async with self._client.stream(
            method=request.method,
            url=url,
            headers=headers,
            content=request.body or None,
        ) as response:
            if response.status_code in RETRYABLE_STATUS_CODES:
                await response.aread()
                resp_headers = dict(response.headers)
                body_text = response.text[:2000]
                body_json = None
                try:
                    body_json = json.loads(response.text)
                except Exception:
                    pass
                retry_after = _parse_retry_after(resp_headers)
                raise RetryableResult(
                    status_code=response.status_code,
                    headers=resp_headers,
                    body_text=body_text,
                    body_json=body_json,
                    retry_after=retry_after,
                    is_stream=True,
                )

            # First chunk: status + headers (no body yet)
            resp_headers = self._filter_response_headers(dict(response.headers))
            await request.response_queue.put(
                StreamChunk(status_code=response.status_code, headers=resp_headers)
            )

            # Body chunks
            async for chunk in response.aiter_bytes():
                if request.cancelled.is_set():
                    logger.info(
                        f"[{request.id}] Client disconnected during stream, aborting"
                    )
                    break
                await request.response_queue.put(StreamChunk(data=chunk))

            # Sentinel
            await request.response_queue.put(StreamChunk(done=True))


# ------------------------------------------------------------------
# Retry-After parser
# ------------------------------------------------------------------

def _parse_retry_after(headers: dict[str, str]) -> Optional[float]:
    """Extract Retry-After header value as seconds.

    Retry-After can be:
      - An integer representing seconds: "Retry-After: 120"
      - An HTTP-date: "Retry-After: Fri, 31 Dec 1999 23:59:59 GMT"

    We handle the integer case (dominant for NVIDIA).  For HTTP-date we
    return None and let the caller use its own backoff.
    """
    raw = headers.get("retry-after") or headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None

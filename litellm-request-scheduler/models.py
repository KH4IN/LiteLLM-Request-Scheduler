"""Data models for LiteLLM Request Scheduler."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class StreamChunk:
    """A chunk of data to be sent back to the client.

    For streaming responses the worker puts multiple chunks:
      1. First chunk carries status_code + headers (no data).
      2. Subsequent chunks carry data bytes.
      3. Final chunk has ``done=True``.

    For non-streaming responses a single chunk carries everything.
    """

    data: Optional[bytes] = None
    status_code: int = 200
    headers: Optional[dict[str, str]] = None
    done: bool = False


@dataclass
class PendingRequest:
    """A request sitting in the FIFO queue waiting for the worker.

    Each instance owns its own ``asyncio.Queue`` so the client handler
    can await the response without mixing results with other requests.
    """

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    method: str = ""
    path: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    query_params: str = ""
    is_stream: bool = False
    response_queue: asyncio.Queue[StreamChunk] = field(
        default_factory=asyncio.Queue
    )
    enqueue_time: float = field(default_factory=time.monotonic)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    processing_started: Optional[float] = None
    retries: int = 0

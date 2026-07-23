"""Test: 429 retry behaviour with Retry-After and rate limiter integration."""

import asyncio
import json
import time
import yaml
import os

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

PROXY_PORT = 18001
MOCK_PORT = 14001
CALL_LOG: list[float] = []


async def main() -> None:
    # ── Mock backend: returns 429 twice, then 200 ───────────────────
    mock = FastAPI()
    attempt_counter = {"count": 0}

    @mock.post("/v1/chat/completions", response_model=None)
    async def mock_chat(request: Request):
        attempt_counter["count"] += 1
        CALL_LOG.append(time.monotonic())
        n = attempt_counter["count"]

        if n <= 2:
            # First two attempts: 429 with Retry-After: 1
            return JSONResponse(
                status_code=429,
                content={"error": {"message": "rate limited by NVIDIA", "type": "rate_limit_error"}},
                headers={"Retry-After": "1", "X-NVIDIA-Request-Id": f"req-{n}"},
            )
        # Third attempt: success
        return JSONResponse({"choices": [{"message": {"role": "assistant", "content": "OK"}}], "model": "mock"})

    mock_cfg = uvicorn.Config(mock, host="127.0.0.1", port=MOCK_PORT, log_level="error")
    mock_server = uvicorn.Server(mock_cfg)
    asyncio.create_task(mock_server.serve())
    await asyncio.sleep(0.3)

    # ── Write test config ───────────────────────────────────────────
    test_config = {
        "proxy": {"host": "127.0.0.1", "port": PROXY_PORT},
        "litellm": {"url": f"http://127.0.0.1:{MOCK_PORT}"},
        "rate_limit": {"requests_per_minute": 300, "burst": 10},
        "retry": {"attempts": 5, "initial_delay": 0.3, "max_delay": 5, "exponential": True},
        "queue": {"max_size": 0},
        "logging": {"level": "WARNING"},
    }
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    backup = open(config_path).read()
    try:
        with open(config_path, "w") as f:
            yaml.dump(test_config, f)

        from app import app
        from logging_config import setup_logging

        setup_logging("WARNING")
        proxy_cfg = uvicorn.Config(app, host="127.0.0.1", port=PROXY_PORT, log_level="error")
        proxy_server = uvicorn.Server(proxy_cfg)
        asyncio.create_task(proxy_server.serve())
        await asyncio.sleep(0.5)

        client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{PROXY_PORT}")

        try:
            # ── Test: 429 → 429 → 200 (retries succeed) ────────────
            print("Test A: 429 retry with Retry-After")
            t0 = time.monotonic()
            r = await client.post("/v1/chat/completions", json={
                "model": "test",
                "messages": [{"role": "user", "content": "Hi"}],
            })
            elapsed = time.monotonic() - t0
            assert r.status_code == 200, f"Expected 200 after retries, got {r.status_code}"
            data = r.json()
            assert data["choices"][0]["message"]["content"] == "OK"
            assert attempt_counter["count"] == 3, f"Expected 3 attempts, got {attempt_counter['count']}"
            print(f"  OK: succeeded after {attempt_counter['count']} attempts in {elapsed:.2f}s")

            # ── Test: Verify rate limiter spacing between retries ───
            print("Test B: Rate limiter spacing between retries")
            if len(CALL_LOG) >= 3:
                gaps = [CALL_LOG[i+1] - CALL_LOG[i] for i in range(len(CALL_LOG)-1)]
                min_interval = 60.0 / 300  # config RPM=300 → 0.2s
                for i, gap in enumerate(gaps):
                    assert gap >= min_interval * 0.8, f"Gap {i}: {gap:.3f}s < {min_interval:.3f}s"
                print(f"  OK: all gaps >= {min_interval:.2f}s (RPM=300)")

            # ── Test: 429 exhaustion → 502 ─────────────────────────
            print("Test C: 429 exhaustion returns 502")
            attempt_counter["count"] = 0  # reset — will always 429

            @mock.post("/v1/exhaust", response_model=None)
            async def mock_exhaust(request: Request):
                return JSONResponse(
                    status_code=429,
                    content={"error": {"message": "NVIDIA rate limit", "type": "rate_limit"}},
                    headers={"Retry-After": "0"},
                )

            r = await client.post("/v1/exhaust", json={"model": "test", "messages": []})
            assert r.status_code == 502, f"Expected 502 after exhaustion, got {r.status_code}"
            data = r.json()
            assert data["error"]["type"] == "proxy_error"
            print(f"  OK: got 502 after exhausting retries")

            # ── Test: Streaming 429 retry ──────────────────────────
            print("Test D: Streaming request with 429 retry")
            stream_counter = {"count": 0}

            @mock.post("/v1/stream_test", response_model=None)
            async def mock_stream(request: Request):
                body = await request.json()
                stream_counter["count"] += 1
                n = stream_counter["count"]
                if n == 1:
                    return JSONResponse(
                        status_code=429,
                        content={"error": {"message": "rate limited"}},
                        headers={"Retry-After": "0"},
                    )
                async def gen():
                    yield b'data: {"choices":[{"delta":{"content":"OK"}}]}\n\n'
                    yield b"data: [DONE]\n\n"
                from fastapi.responses import StreamingResponse
                return StreamingResponse(gen(), media_type="text/event-stream")

            chunks = []
            async with client.stream("POST", "/v1/stream_test", json={"model": "test", "messages": [], "stream": True}) as r:
                assert r.status_code == 200, f"Expected 200, got {r.status_code}"
                async for line in r.aiter_lines():
                    if line.startswith("data: "):
                        chunks.append(line)
            assert len(chunks) >= 1
            print(f"  OK: streaming succeeded after retry, got {len(chunks)} chunks")

            print()
            print("=== ALL 4 RETRY TESTS PASSED ===")

        finally:
            await client.aclose()
            proxy_server.should_exit = True

    finally:
        mock_server.should_exit = True
        with open(config_path, "w") as f:
            f.write(backup)


if __name__ == "__main__":
    asyncio.run(main())

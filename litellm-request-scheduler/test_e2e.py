"""End-to-end integration test with a mock LiteLLM backend."""

import asyncio
import json
import tempfile
import os

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

MOCK_PORT = 14000
PROXY_PORT = 18000


async def main() -> None:
    # ── Mock LiteLLM backend on :14000 ─────────────────────────────
    mock = FastAPI()

    @mock.post("/v1/chat/completions", response_model=None)
    async def mock_chat(request: Request):
        body = await request.json()
        if body.get("stream"):
            async def gen():
                yield b'data: ' + json.dumps({"choices": [{"delta": {"content": "Hello"}}]}).encode() + b"\n\n"
                yield b'data: ' + json.dumps({"choices": [{"delta": {"content": " world"}}]}).encode() + b"\n\n"
                yield b"data: [DONE]\n\n"
            return StreamingResponse(gen(), media_type="text/event-stream")
        return JSONResponse({"choices": [{"message": {"role": "assistant", "content": "Hi there!"}}], "model": "mock"})

    @mock.get("/v1/models")
    async def mock_models():
        return JSONResponse({"data": [{"id": "mock-model", "object": "model"}]})

    mock_cfg = uvicorn.Config(mock, host="127.0.0.1", port=MOCK_PORT, log_level="error")
    mock_server = uvicorn.Server(mock_cfg)
    asyncio.create_task(mock_server.serve())
    await asyncio.sleep(0.3)

    # ── Queue proxy on :18000 → mock :14000 ────────────────────────
    # Write a test-specific config so the proxy points to our mock
    import yaml
    test_config = {
        "proxy": {"host": "127.0.0.1", "port": PROXY_PORT},
        "litellm": {"url": f"http://127.0.0.1:{MOCK_PORT}"},
        "rate_limit": {"requests_per_minute": 300, "burst": 10},
        "retry": {"attempts": 3, "initial_delay": 0.5, "max_delay": 5, "exponential": True},
        "queue": {"max_size": 0},
        "logging": {"level": "WARNING"},
    }
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    backup = open(config_path).read()
    try:
        with open(config_path, "w") as f:
            yaml.dump(test_config, f)

        from app import app  # noqa: E402 — re-import with updated config
        from logging_config import setup_logging  # noqa: E402

        setup_logging("WARNING")
        proxy_cfg = uvicorn.Config(app, host="127.0.0.1", port=PROXY_PORT, log_level="error")
        proxy_server = uvicorn.Server(proxy_cfg)
        asyncio.create_task(proxy_server.serve())
        await asyncio.sleep(0.5)

        client = httpx.AsyncClient(base_url=f"http://127.0.0.1:{PROXY_PORT}")

        try:
            # Test 1: Non-streaming chat completion
            print("Test 1: Non-streaming chat completion")
            r = await client.post("/v1/chat/completions", json={"model": "test", "messages": [{"role": "user", "content": "Hi"}]})
            assert r.status_code == 200, f"Expected 200, got {r.status_code}"
            data = r.json()
            assert data["choices"][0]["message"]["content"] == "Hi there!"
            print(f"  OK: {data['choices'][0]['message']['content']}")

            # Test 2: GET /v1/models
            print("Test 2: GET /v1/models")
            r = await client.get("/v1/models")
            assert r.status_code == 200
            data = r.json()
            assert len(data["data"]) == 1
            print(f"  OK: {data['data'][0]['id']}")

            # Test 3: Streaming chat completion
            print("Test 3: Streaming chat completion")
            chunks: list[str] = []
            async with client.stream("POST", "/v1/chat/completions", json={"model": "test", "messages": [{"role": "user", "content": "Hi"}], "stream": True}) as r:
                assert r.status_code == 200, f"Expected 200, got {r.status_code}"
                assert "text/event-stream" in r.headers.get("content-type", "")
                async for line in r.aiter_lines():
                    if line.startswith("data: "):
                        chunks.append(line)
            print(f"  OK: received {len(chunks)} SSE chunks")

            # Test 4: /health
            print("Test 4: /health")
            r = await client.get("/health")
            assert r.status_code == 200
            health = r.json()
            assert health["status"] == "ok"
            assert health["processed_requests"] >= 3
            print(f"  OK: processed={health['processed_requests']} uptime={health['uptime']}")

            # Test 5: /metrics
            print("Test 5: /metrics")
            r = await client.get("/metrics")
            assert r.status_code == 200
            metrics = r.json()
            assert metrics["status"] == "ok"
            print(f"  OK: rpm={metrics['current_rpm']} avg_wait={metrics['average_wait_seconds']}s")

            # Test 6: Concurrent FIFO requests
            print("Test 6: Concurrent FIFO requests")
            tasks = [
                client.post("/v1/chat/completions", json={"model": "test", "messages": [{"role": "user", "content": f"Msg {i}"}]})
                for i in range(5)
            ]
            results = await asyncio.gather(*tasks)
            for i, res in enumerate(results):
                assert res.status_code == 200, f"Request {i} failed: {res.status_code}"
            print(f"  OK: all 5 concurrent requests succeeded")

            # Test 7: Unknown endpoint passthrough
            print("Test 7: Unknown /v1/ endpoint passthrough")
            r = await client.get("/v1/nonexistent")
            assert r.status_code in (404, 405), f"Expected 404/405, got {r.status_code}"
            print(f"  OK: got {r.status_code} as expected")

            print()
            print("=== ALL 7 TESTS PASSED ===")

        finally:
            await client.aclose()
            proxy_server.should_exit = True

    finally:
        mock_server.should_exit = True
        with open(config_path, "w") as f:
            f.write(backup)


if __name__ == "__main__":
    asyncio.run(main())

"""Configuration management for LiteLLM Request Scheduler."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ProxyConfig(BaseModel):
    """HTTP proxy server configuration."""

    host: str = "0.0.0.0"
    port: int = 8000


class LiteLLMConfig(BaseModel):
    """LiteLLM backend configuration."""

    url: str = "http://localhost:4000"


class RateLimitConfig(BaseModel):
    """Fixed-rate spacing limiter configuration.

    The limiter enforces exact minimum intervals between requests:
    ``60 / requests_per_minute`` seconds.  Aligned with NVIDIA's
    sliding-window rate limit.
    """

    algorithm: str = "fixed_spacing"
    requests_per_minute: int = 35
    burst: int = 1  # kept for backward compat, unused by FixedRateLimiter


class RetryConfig(BaseModel):
    """Retry policy for failed requests."""

    attempts: int = 5
    initial_delay: float = 2.0
    max_delay: float = 30.0
    exponential: bool = True


class QueueConfig(BaseModel):
    """Queue configuration."""

    max_size: int = 0  # 0 = unlimited


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = "INFO"


class AppConfig(BaseModel):
    """Root application configuration."""

    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    litellm: LiteLLMConfig = Field(default_factory=LiteLLMConfig)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    queue: QueueConfig = Field(default_factory=QueueConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(path: str = "config.yaml") -> AppConfig:
    """Load configuration from YAML file with fallback to defaults.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated application configuration.

    Raises:
        SystemExit: If the config file exists but cannot be parsed.
    """
    config_path = Path(path)
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                data = yaml.safe_load(f) or {}
            return AppConfig(**data)
        except Exception as e:
            logger.critical(f"Failed to load config from {path}: {e}")
            raise SystemExit(1) from e
    logger.info(f"No config file found at {path}, using defaults")
    return AppConfig()

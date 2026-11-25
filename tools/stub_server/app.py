"""Application factory for the in-memory HTTP contract stub."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from dvs.api.app import create_app as create_api_app
from dvs.config import Config, load_config

from .backend import StubBackend

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG = _REPOSITORY_ROOT / "deploy" / "ray" / "config.local.yaml"


def create_stub_app(config: Config | None = None) -> FastAPI:
    """Build the contract API with one in-memory backend and the local development config."""
    if config is None:
        config_path = os.environ.get("DVS_CONFIG") or _DEFAULT_CONFIG
        config = load_config(config_path)
    return create_api_app(config, StubBackend(config.collection_specs))

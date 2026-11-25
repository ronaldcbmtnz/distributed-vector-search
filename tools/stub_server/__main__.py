"""Run the single-process contract stub with ``python -m tools.stub_server``."""

from __future__ import annotations

import argparse
import os

import uvicorn

from .app import create_stub_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the in-memory DVS contract stub")
    parser.add_argument("--host", default=os.environ.get("DVS_STUB_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("DVS_STUB_PORT", "8080"))
    )
    args = parser.parse_args()
    uvicorn.run(create_stub_app(), host=args.host, port=args.port)


if __name__ == "__main__":
    main()

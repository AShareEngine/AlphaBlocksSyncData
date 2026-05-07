#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Start API service."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start AlphaBlocksSyncData API service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8010)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    install_sync_data_system_alias(PROJECT_ROOT)
    from sync_data_system.service.api import app

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=False,
        access_log=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

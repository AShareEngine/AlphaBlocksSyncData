#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Execute a persisted cross-provider sync task batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.service.task_batch import run_task_batch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a cross-provider sync task batch")
    parser.add_argument("--payload", required=True)
    parser.add_argument("--results", required=True)
    parser.add_argument("--log-path", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    return run_task_batch(
        payload,
        results_path=Path(args.results),
        log_path=Path(args.log_path),
    )


if __name__ == "__main__":
    raise SystemExit(main())

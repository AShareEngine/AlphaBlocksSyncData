#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate provider sync plan TOML files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.sync_plan import discover_sync_plan_paths, validate_sync_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate run_sync*.toml provider sync configs.")
    parser.add_argument("configs", nargs="*", help="TOML sync plan paths. Defaults to config/sync/plans/run_sync*.toml.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = [Path(item) for item in args.configs] if args.configs else discover_sync_plan_paths(PROJECT_ROOT)
    if not paths:
        raise ValueError("no sync configs found")

    total_tasks = 0
    total_enabled = 0
    failed = 0
    for path in paths:
        try:
            result = validate_sync_plan(path, project_root=PROJECT_ROOT)
        except Exception as exc:
            failed += 1
            print(f"[FAIL] config={path} error={exc}")
            continue
        total_tasks += result.total_tasks
        total_enabled += result.enabled_tasks
        print(
            f"[OK] config={result.path} source={result.source} "
            f"tasks={result.total_tasks} enabled={result.enabled_tasks} disabled={result.disabled_tasks}"
        )

    print(
        f"[SUMMARY] configs={len(paths)} failed={failed} "
        f"tasks={total_tasks} enabled={total_enabled}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)

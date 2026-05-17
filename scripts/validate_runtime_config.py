#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate runtime.local.yaml without printing secrets."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.config_paths import DEFAULT_RUNTIME_CONFIG_PATH, resolve_runtime_config_path
from sync_data_system.runtime_config import RuntimeConfig, load_runtime_config


PROVIDERS = {"amazingdata", "baostock", "qmt"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate runtime.local.yaml required fields.")
    parser.add_argument("runtime_path", nargs="?", default=str(DEFAULT_RUNTIME_CONFIG_PATH))
    parser.add_argument("--provider", action="append", default=[], help="Provider to validate. Can be repeated.")
    parser.add_argument("--all", action="store_true", help="Validate every provider section.")
    parser.add_argument("--allow-placeholders", action="store_true", help="Allow YOUR_* placeholders, useful for runtime.example.yaml.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = resolve_runtime_config_path(args.runtime_path)
    runtime = load_runtime_config(path)
    providers = _resolve_providers(args.provider, include_all=bool(args.all))
    errors = validate_runtime_config(runtime, providers=providers, allow_placeholders=bool(args.allow_placeholders))
    if errors:
        for error in errors:
            print(f"[FAIL] {error}")
        print(f"[SUMMARY] runtime={path} providers={','.join(providers)} failed={len(errors)}")
        return 1
    print(f"[OK] runtime={path} providers={','.join(providers)}")
    return 0


def validate_runtime_config(
    runtime: RuntimeConfig,
    *,
    providers: Iterable[str],
    allow_placeholders: bool = False,
) -> list[str]:
    errors: list[str] = []
    _require(runtime.datasource.host, "datasource.host", errors, allow_placeholders=allow_placeholders)
    _require(runtime.datasource.database, "datasource.database", errors, allow_placeholders=allow_placeholders)
    _require(runtime.datasource.username, "datasource.username", errors, allow_placeholders=allow_placeholders)

    provider_set = set(providers)
    if "amazingdata" in provider_set:
        _require(runtime.sync.amazingdata.username, "sync.amazingdata.username", errors, allow_placeholders=allow_placeholders)
        _require(runtime.sync.amazingdata.password, "sync.amazingdata.password", errors, allow_placeholders=allow_placeholders)
        _require(runtime.sync.amazingdata.host, "sync.amazingdata.host", errors, allow_placeholders=allow_placeholders)
        _require(runtime.sync.amazingdata.port, "sync.amazingdata.port", errors, allow_placeholders=allow_placeholders)
        _require(runtime.sync.amazingdata.local_path, "sync.amazingdata.local_path", errors, allow_placeholders=allow_placeholders)
    if "qmt" in provider_set:
        _require(runtime.sync.qmt.base_url, "sync.qmt.base_url", errors, allow_placeholders=allow_placeholders)
        _require(runtime.sync.qmt.api_key, "sync.qmt.api_key", errors, allow_placeholders=allow_placeholders)
    return errors


def _resolve_providers(provider_args: list[str], *, include_all: bool) -> tuple[str, ...]:
    requested = {str(item).strip() for item in provider_args if str(item).strip()}
    if include_all:
        if requested:
            raise ValueError("不要同时传 --provider 和 --all。")
        return tuple(sorted(PROVIDERS))
    if not requested:
        return tuple(sorted(PROVIDERS))
    missing = requested - PROVIDERS
    if missing:
        raise ValueError(f"unknown provider(s): {sorted(missing)}; available={sorted(PROVIDERS)}")
    return tuple(sorted(requested))


def _require(value, field_name: str, errors: list[str], *, allow_placeholders: bool) -> None:
    text = str(value or "").strip()
    if not text or text in {"0", "None"}:
        errors.append(f"{field_name} is required")
        return
    if allow_placeholders:
        return
    if "YOUR_" in text or text.startswith("/path/to/"):
        errors.append(f"{field_name} still uses a placeholder")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)

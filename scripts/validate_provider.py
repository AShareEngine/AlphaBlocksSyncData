#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate provider manifests and lightweight entrypoints."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.providers import ProviderManifest, load_provider_registry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate sync provider manifests.")
    parser.add_argument("--provider", action="append", default=[], help="Only validate this provider. Can be repeated.")
    parser.add_argument("--load-entrypoints", action="store_true", help="Import configured entrypoint objects.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_provider_registry()
    requested = {str(item).strip() for item in args.provider if str(item).strip()}
    manifests = registry.list()
    if requested:
        missing = sorted(requested - set(registry.names()))
        if missing:
            raise ValueError(f"unknown provider(s): {missing}; available={registry.names()}")
        manifests = [manifest for manifest in manifests if manifest.name in requested]

    for manifest in manifests:
        validate_manifest(manifest, load_entrypoints=bool(args.load_entrypoints))
        print(
            f"[OK] provider={manifest.name} tasks={len(manifest.tasks)} "
            f"module={manifest.module} manifest={manifest.manifest_path}"
        )
    print(f"[SUMMARY] providers={len(manifests)}")
    return 0


def validate_manifest(manifest: ProviderManifest, *, load_entrypoints: bool) -> None:
    if not manifest.tasks:
        raise ValueError(f"{manifest.name}: provider must expose at least one task")
    task_names = [task.name for task in manifest.tasks]
    if len(task_names) != len(set(task_names)):
        raise ValueError(f"{manifest.name}: duplicate task names")
    for task in manifest.tasks:
        if not task.target:
            raise ValueError(f"{manifest.name}.{task.name}: target is required")
        if task.supports_incremental and not task.cursor_field:
            raise ValueError(f"{manifest.name}.{task.name}: cursor_field is required for incremental tasks")
    if not load_entrypoints:
        return
    if manifest.entrypoints.provider:
        manifest.load_object(manifest.entrypoints.provider)
    if manifest.entrypoints.repository:
        manifest.load_object(manifest.entrypoints.repository)
    if manifest.entrypoints.specs:
        manifest.load_object(manifest.entrypoints.specs)
    if manifest.entrypoints.context_builder:
        manifest.load_context_builder()
    if manifest.entrypoints.registered_task_runner:
        manifest.load_registered_task_runner()
    manifest.load_config_runner()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)

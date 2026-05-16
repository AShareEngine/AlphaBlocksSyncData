#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Install or check dependencies declared by provider manifests."""

from __future__ import annotations

import argparse
import importlib.util
import importlib.metadata
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.core.providers import ProviderManifest, load_provider_registry


PACKAGE_NAME_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install dependencies declared in providers/<name>/provider.toml.")
    parser.add_argument("providers", nargs="*", help="Provider names. Omit with --all to include every provider.")
    parser.add_argument("--all", action="store_true", help="Include all providers.")
    parser.add_argument("--check", action="store_true", help="Only check whether dependencies are importable/installed.")
    parser.add_argument("--dry-run", action="store_true", help="Print pip commands without running them.")
    parser.add_argument("--install", action="store_true", help="Run pip install for missing dependencies.")
    parser.add_argument("--upgrade", action="store_true", help="Pass --upgrade to pip install.")
    parser.add_argument("--python", default=sys.executable, help="Python executable used to run pip.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    registry = load_provider_registry()
    manifests = resolve_manifests(args.providers, include_all=bool(args.all))
    dependencies = dedupe_dependencies(manifests)
    import_modules = dedupe_import_modules(manifests)

    if not dependencies and not import_modules:
        print("[SUMMARY] dependencies=0 import_modules=0")
        return 0

    missing = [item for item in dependencies if not is_installed(item)]
    for item in dependencies:
        status = "installed" if item not in missing else "missing"
        print(f"[{status.upper()}] {item}")

    missing_modules = [item for item in import_modules if not is_importable(item)]
    for item in import_modules:
        status = "importable" if item not in missing_modules else "missing"
        print(f"[{status.upper()}] import {item}")

    if args.check:
        print(
            f"[SUMMARY] dependencies={len(dependencies)} missing={len(missing)} "
            f"import_modules={len(import_modules)} missing_imports={len(missing_modules)}"
        )
        return 1 if missing or missing_modules else 0

    command = build_pip_command(args.python, missing, upgrade=bool(args.upgrade))
    if args.dry_run or not args.install:
        if command:
            print("[DRY] " + " ".join(command))
        else:
            print("[DRY] no missing dependencies")
        print(
            f"[SUMMARY] dependencies={len(dependencies)} missing={len(missing)} "
            f"import_modules={len(import_modules)} missing_imports={len(missing_modules)}"
        )
        return 0

    if not command:
        print(
            "[SUMMARY] dependencies=%s missing=0 installed=0 import_modules=%s missing_imports=%s"
            % (len(dependencies), len(import_modules), len(missing_modules))
        )
        return 0
    subprocess.run(command, check=True)
    print(
        f"[SUMMARY] dependencies={len(dependencies)} installed={len(missing)} "
        f"import_modules={len(import_modules)} missing_imports={len(missing_modules)}"
    )
    return 0


def resolve_manifests(provider_names: list[str], *, include_all: bool) -> list[ProviderManifest]:
    registry = load_provider_registry()
    names = [str(item).strip() for item in provider_names if str(item).strip()]
    if include_all:
        if names:
            raise ValueError("不要同时传 provider 名称和 --all。")
        return registry.list()
    if not names:
        raise ValueError("请传 provider 名称，或使用 --all。")
    missing = sorted(set(names) - set(registry.names()))
    if missing:
        raise ValueError(f"unknown provider(s): {missing}; available={registry.names()}")
    return [registry.get(name) for name in names]


def dedupe_dependencies(manifests: list[ProviderManifest]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for manifest in manifests:
        for dependency in manifest.dependencies:
            text = str(dependency).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)
    return result


def dedupe_import_modules(manifests: list[ProviderManifest]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for manifest in manifests:
        for module_name in manifest.import_modules:
            text = str(module_name).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)
    return result


def is_installed(requirement: str) -> bool:
    package_name = requirement_package_name(requirement)
    if not package_name:
        return False
    try:
        importlib.metadata.version(package_name)
        return True
    except importlib.metadata.PackageNotFoundError:
        return False


def is_importable(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def requirement_package_name(requirement: str) -> str:
    match = PACKAGE_NAME_RE.match(requirement)
    return match.group(1).replace("_", "-") if match else ""


def build_pip_command(python_executable: str, dependencies: list[str], *, upgrade: bool) -> list[str]:
    if not dependencies:
        return []
    command = [python_executable, "-m", "pip", "install"]
    if upgrade:
        command.append("--upgrade")
    command.extend(dependencies)
    return command


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)

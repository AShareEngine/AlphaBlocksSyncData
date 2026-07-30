#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Runtime helpers for running AlphaBlocksSyncData as a program."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def install_sync_data_system_alias(project_root: Path | None = None) -> Path:
    """Map legacy ``sync_data_system.*`` imports to this program directory."""

    root = Path(project_root or Path(__file__).resolve().parent).resolve()
    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)

    existing = sys.modules.get("sync_data_system")
    if existing is not None:
        module_paths = [str(Path(item).resolve()) for item in getattr(existing, "__path__", [])]
        if root_text in module_paths:
            return root

    init_path = root / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        "sync_data_system",
        init_path,
        submodule_search_locations=[root_text],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to create sync_data_system alias for {root}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["sync_data_system"] = module
    spec.loader.exec_module(module)
    # A test or embedded host may reload only the package object while keeping
    # already-imported children in sys.modules. Reattach direct children so
    # dotted lookups (including unittest.mock.patch) keep working.
    prefix = "sync_data_system."
    for module_name, child in tuple(sys.modules.items()):
        if not module_name.startswith(prefix):
            continue
        relative_name = module_name[len(prefix) :]
        if "." not in relative_name and child is not None:
            setattr(module, relative_name, child)
    return root


__all__ = ["install_sync_data_system_alias"]

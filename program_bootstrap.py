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
    return root


__all__ = ["install_sync_data_system_alias"]

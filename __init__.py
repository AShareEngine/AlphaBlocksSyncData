from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODULES = {
    "run_sync": "sync_data_system.run_sync",
    "wide_table_sync": "sync_data_system.wide_table_sync",
}

__all__ = sorted(_LAZY_MODULES)


def __getattr__(name: str) -> Any:
    if name in _LAZY_MODULES:
        return import_module(_LAZY_MODULES[name])
    raise AttributeError(f"module 'sync_data_system' has no attribute {name!r}")

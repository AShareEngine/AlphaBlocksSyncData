#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Provider manifest loading and lightweight provider registry."""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from sync_data_system.config_paths import resolve_provider_root
from sync_data_system.toml_compat import tomllib


MANIFEST_FILENAME = "provider.toml"


@dataclass(frozen=True)
class ProviderEntrypoints:
    provider: str = ""
    repository: str = ""
    runner: str = ""
    config_runner: str = ""
    context_builder: str = ""
    registered_task_runner: str = ""
    specs: str = ""


@dataclass(frozen=True)
class ProviderTaskManifest:
    name: str
    target: str
    supports_incremental: bool = False
    cursor_field: str = ""
    request_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProviderManifest:
    name: str
    display_name: str
    version: str
    module: str
    runtime_config_key: str
    default_database: str
    dependencies: tuple[str, ...]
    import_modules: tuple[str, ...]
    entrypoints: ProviderEntrypoints
    tasks: tuple[ProviderTaskManifest, ...]
    manifest_path: Path
    root: Path
    plans_path: Path | None = None

    @property
    def task_names(self) -> tuple[str, ...]:
        return tuple(task.name for task in self.tasks)

    def load_object(self, spec: str) -> Any:
        if not spec:
            raise ValueError(f"provider {self.name!r} entrypoint is empty")
        module_name, separator, attr_name = spec.partition(":")
        if not separator or not module_name or not attr_name:
            raise ValueError(f"provider {self.name!r} entrypoint must use module:object format: {spec!r}")
        if module_name.startswith("."):
            import_name = f"{self.module}{module_name}"
        elif "." not in module_name:
            import_name = f"{self.module}.{module_name}"
        else:
            import_name = module_name
        module = importlib.import_module(import_name)
        obj: Any = module
        for part in attr_name.split("."):
            obj = getattr(obj, part)
        return obj

    def load_config_runner(self) -> Callable[..., int]:
        runner_spec = self.entrypoints.config_runner or self.entrypoints.runner
        runner = self.load_object(runner_spec)
        if not callable(runner):
            raise TypeError(f"provider {self.name!r} config runner is not callable: {runner_spec!r}")
        return runner

    def load_context_builder(self) -> Callable[..., Any]:
        builder = self.load_object(self.entrypoints.context_builder)
        if not callable(builder):
            raise TypeError(f"provider {self.name!r} context builder is not callable: {self.entrypoints.context_builder!r}")
        return builder

    def load_registered_task_runner(self) -> Callable[..., int]:
        runner = self.load_object(self.entrypoints.registered_task_runner)
        if not callable(runner):
            raise TypeError(
                f"provider {self.name!r} registered task runner is not callable: {self.entrypoints.registered_task_runner!r}"
            )
        return runner


class ProviderRegistry:
    def __init__(self, manifests: Iterable[ProviderManifest] = ()) -> None:
        self._providers: dict[str, ProviderManifest] = {}
        for manifest in manifests:
            self.register(manifest)

    def register(self, manifest: ProviderManifest) -> None:
        if manifest.name in self._providers:
            previous = self._providers[manifest.name]
            raise ValueError(
                f"duplicate provider {manifest.name!r}: {previous.manifest_path} and {manifest.manifest_path}"
            )
        self._providers[manifest.name] = manifest

    def get(self, name: str) -> ProviderManifest:
        provider_name = str(name or "").strip()
        if provider_name not in self._providers:
            raise KeyError(provider_name)
        return self._providers[provider_name]

    def has(self, name: str) -> bool:
        return str(name or "").strip() in self._providers

    def names(self) -> list[str]:
        return sorted(self._providers)

    def list(self) -> list[ProviderManifest]:
        return [self._providers[name] for name in self.names()]

    def to_metadata(self) -> list[dict[str, Any]]:
        return [provider_manifest_to_dict(manifest) for manifest in self.list()]


def load_provider_registry(project_root: str | Path | None = None) -> ProviderRegistry:
    provider_root = resolve_provider_root(project_root)
    manifests: list[ProviderManifest] = []
    if provider_root.exists():
        for manifest_path in sorted(provider_root.glob(f"*/{MANIFEST_FILENAME}")):
            manifests.append(load_provider_manifest(manifest_path))
    return ProviderRegistry(manifests)


def load_provider_manifest(path: str | Path) -> ProviderManifest:
    manifest_path = Path(path).expanduser().resolve()
    if not manifest_path.is_file():
        raise FileNotFoundError(f"provider manifest not found: {manifest_path}")
    with manifest_path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{manifest_path}: top-level TOML value must be a table")

    allowed_keys = {
        "name",
        "display_name",
        "version",
        "module",
        "runtime_config_key",
        "default_database",
        "dependencies",
        "import_modules",
        "entrypoints",
        "tasks",
        "plans_path",
    }
    unexpected = set(data) - allowed_keys
    if unexpected:
        raise ValueError(f"{manifest_path}: unknown provider fields: {sorted(unexpected)}")

    name = _required_string(data, "name", manifest_path)
    root = manifest_path.parent
    plans_path_value = str(data.get("plans_path") or "").strip()
    plans_path = (root / plans_path_value).resolve() if plans_path_value else None
    return ProviderManifest(
        name=name,
        display_name=_optional_string(data, "display_name", name),
        version=_optional_string(data, "version", "0.1.0"),
        module=_required_string(data, "module", manifest_path),
        runtime_config_key=_optional_string(data, "runtime_config_key", name),
        default_database=_optional_string(data, "default_database", name),
        dependencies=_string_tuple(data.get("dependencies"), field_name="dependencies", manifest_path=manifest_path),
        import_modules=_string_tuple(data.get("import_modules"), field_name="import_modules", manifest_path=manifest_path),
        entrypoints=_parse_entrypoints(data.get("entrypoints"), manifest_path=manifest_path),
        tasks=_parse_tasks(data.get("tasks"), manifest_path=manifest_path),
        manifest_path=manifest_path,
        root=root,
        plans_path=plans_path,
    )


def provider_manifest_to_dict(manifest: ProviderManifest) -> dict[str, Any]:
    return {
        "name": manifest.name,
        "display_name": manifest.display_name,
        "version": manifest.version,
        "module": manifest.module,
        "runtime_config_key": manifest.runtime_config_key,
        "default_database": manifest.default_database,
        "dependencies": list(manifest.dependencies),
        "import_modules": list(manifest.import_modules),
        "entrypoints": {
            "provider": manifest.entrypoints.provider,
            "repository": manifest.entrypoints.repository,
            "runner": manifest.entrypoints.runner,
            "config_runner": manifest.entrypoints.config_runner,
            "context_builder": manifest.entrypoints.context_builder,
            "registered_task_runner": manifest.entrypoints.registered_task_runner,
            "specs": manifest.entrypoints.specs,
        },
        "tasks": [
            {
                "name": task.name,
                "target": task.target,
                "supports_incremental": task.supports_incremental,
                "cursor_field": task.cursor_field,
                "request_fields": list(task.request_fields),
            }
            for task in manifest.tasks
        ],
        "manifest_path": str(manifest.manifest_path),
        "plans_path": str(manifest.plans_path) if manifest.plans_path else None,
    }


def _parse_entrypoints(value: Any, *, manifest_path: Path) -> ProviderEntrypoints:
    if not isinstance(value, dict):
        raise ValueError(f"{manifest_path}: [entrypoints] is required")
    allowed = {"provider", "repository", "runner", "config_runner", "context_builder", "registered_task_runner", "specs"}
    unexpected = set(value) - allowed
    if unexpected:
        raise ValueError(f"{manifest_path}: unknown entrypoint fields: {sorted(unexpected)}")
    return ProviderEntrypoints(
        provider=str(value.get("provider") or "").strip(),
        repository=str(value.get("repository") or "").strip(),
        runner=str(value.get("runner") or "").strip(),
        config_runner=str(value.get("config_runner") or "").strip(),
        context_builder=str(value.get("context_builder") or "").strip(),
        registered_task_runner=str(value.get("registered_task_runner") or "").strip(),
        specs=str(value.get("specs") or "").strip(),
    )


def _parse_tasks(value: Any, *, manifest_path: Path) -> tuple[ProviderTaskManifest, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{manifest_path}: [[tasks]] must be an array of tables")
    tasks: list[ProviderTaskManifest] = []
    seen: set[str] = set()
    allowed = {"name", "target", "supports_incremental", "cursor_field", "request_fields"}
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"{manifest_path}: tasks[{index}] must be a table")
        unexpected = set(item) - allowed
        if unexpected:
            raise ValueError(f"{manifest_path}: tasks[{index}] unknown fields: {sorted(unexpected)}")
        name = _required_string(item, "name", manifest_path, prefix=f"tasks[{index}].")
        if name in seen:
            raise ValueError(f"{manifest_path}: duplicate task name: {name!r}")
        seen.add(name)
        tasks.append(
            ProviderTaskManifest(
                name=name,
                target=_required_string(item, "target", manifest_path, prefix=f"tasks[{index}]."),
                supports_incremental=_optional_bool(item, "supports_incremental", False, manifest_path),
                cursor_field=_optional_string(item, "cursor_field", ""),
                request_fields=_string_tuple(
                    item.get("request_fields"),
                    field_name=f"tasks[{index}].request_fields",
                    manifest_path=manifest_path,
                ),
            )
        )
    return tuple(tasks)


def _required_string(data: dict[str, Any], key: str, manifest_path: Path, prefix: str = "") -> str:
    value = str(data.get(key) or "").strip()
    if not value:
        raise ValueError(f"{manifest_path}: {prefix}{key} is required")
    return value


def _optional_string(data: dict[str, Any], key: str, default: str) -> str:
    value = data.get(key)
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _optional_bool(data: dict[str, Any], key: str, default: bool, manifest_path: Path) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{manifest_path}: {key} must be boolean")
    return value


def _string_tuple(value: Any, *, field_name: str, manifest_path: Path) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{manifest_path}: {field_name} must be an array")
    return tuple(str(item).strip() for item in value if str(item).strip())


__all__ = [
    "ProviderEntrypoints",
    "ProviderManifest",
    "ProviderRegistry",
    "ProviderTaskManifest",
    "load_provider_manifest",
    "load_provider_registry",
    "provider_manifest_to_dict",
]

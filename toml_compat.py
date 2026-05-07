#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""轻量 TOML 兼容层.

优先使用 `tomllib` / `tomli`；
若环境缺失，则退回到仅覆盖当前项目配置文件语法的最小解析器。
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

try:
    import tomllib as _stdlib_tomllib  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    _stdlib_tomllib = None

if _stdlib_tomllib is None:  # pragma: no cover
    try:
        import tomli as _stdlib_tomllib  # type: ignore[assignment]
    except Exception:  # pragma: no cover
        _stdlib_tomllib = None


def _strip_comment(line: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    result: list[str] = []
    for ch in line:
        if escaped:
            result.append(ch)
            escaped = False
            continue
        if ch == "\\" and in_double:
            result.append(ch)
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            result.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            result.append(ch)
            continue
        if ch == "#" and not in_single and not in_double:
            break
        result.append(ch)
    return "".join(result).strip()


def _split_array_items(text: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    escaped = False
    depth = 0
    for ch in text:
        if escaped:
            current.append(ch)
            escaped = False
            continue
        if ch == "\\" and in_double:
            current.append(ch)
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            current.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            current.append(ch)
            continue
        if ch == "[" and not in_single and not in_double:
            depth += 1
        elif ch == "]" and not in_single and not in_double and depth > 0:
            depth -= 1
        if ch == "," and not in_single and not in_double and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(item)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def _parse_value(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_parse_value(item) for item in _split_array_items(inner)]
    try:
        return ast.literal_eval(text)
    except Exception:
        pass
    try:
        return int(text)
    except Exception:
        return text


def _fallback_loads(text: str) -> dict[str, Any]:
    data: dict[str, Any] = {}
    current: dict[str, Any] = data
    for raw_line in text.splitlines():
        line = _strip_comment(raw_line)
        if not line:
            continue
        if line.startswith("[[") and line.endswith("]]"):
            section = line[2:-2].strip()
            if section != "tasks":
                raise ValueError(f"fallback TOML parser 暂不支持 [[{section}]]")
            tasks = data.setdefault("tasks", [])
            if not isinstance(tasks, list):
                raise ValueError("tasks 必须是数组。")
            current = {}
            tasks.append(current)
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            if "." in section:
                raise ValueError(f"fallback TOML parser 暂不支持嵌套 table: [{section}]")
            table = data.setdefault(section, {})
            if not isinstance(table, dict):
                raise ValueError(f"{section} 必须是 table。")
            current = table
            continue
        if "=" not in line:
            raise ValueError(f"无法解析 TOML 行: {raw_line!r}")
        key, raw_value = line.split("=", 1)
        current[key.strip()] = _parse_value(raw_value.strip())
    return data


@dataclass(frozen=True)
class _TomlCompatModule:
    def load(self, fp) -> dict[str, Any]:
        raw = fp.read()
        if isinstance(raw, bytes):
            text = raw.decode("utf-8")
        else:
            text = str(raw)
        if _stdlib_tomllib is not None:
            if isinstance(raw, bytes):
                try:
                    fp.seek(0)
                    return _stdlib_tomllib.load(fp)
                except Exception:
                    pass
            return _stdlib_tomllib.loads(text)
        return _fallback_loads(text)

    def loads(self, text: str) -> dict[str, Any]:
        if _stdlib_tomllib is not None:
            return _stdlib_tomllib.loads(text)
        return _fallback_loads(text)


tomllib = _TomlCompatModule()


__all__ = ["tomllib"]

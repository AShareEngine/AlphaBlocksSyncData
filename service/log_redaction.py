#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Redact authentication secrets emitted by third-party SDKs."""

from __future__ import annotations

import re
import sys
from typing import Any, TextIO


_JSON_TOKEN_RE = re.compile(
    r'("(?:token|access_token|refresh_token)"\s*:\s*")[^"]*(")',
    flags=re.IGNORECASE,
)
_TEXT_TOKEN_RE = re.compile(
    r"(\b(?:token|access_token|refresh_token)\b\s*[:=]\s*)(?!\[REDACTED\])([^\s,;]+)",
    flags=re.IGNORECASE,
)
_BEARER_TOKEN_RE = re.compile(
    r"(\bBearer\s+)(?!\[REDACTED\])([A-Za-z0-9._~+/=-]+)",
    flags=re.IGNORECASE,
)


def redact_sensitive_text(value: Any) -> str:
    """Return text with common authentication token formats removed."""

    text = str(value)
    text = _JSON_TOKEN_RE.sub(r"\1[REDACTED]\2", text)
    text = _TEXT_TOKEN_RE.sub(r"\1[REDACTED]", text)
    return _BEARER_TOKEN_RE.sub(r"\1[REDACTED]", text)


class RedactingTextStream:
    """Minimal text-stream proxy that redacts secrets before writing."""

    def __init__(self, wrapped: TextIO) -> None:
        self._wrapped = wrapped

    def write(self, value: str) -> int:
        return self._wrapped.write(redact_sensitive_text(value))

    def flush(self) -> None:
        self._wrapped.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wrapped, name)


def install_output_redaction() -> None:
    """Wrap stdout/stderr once so SDK print output cannot expose tokens."""

    if not isinstance(sys.stdout, RedactingTextStream):
        sys.stdout = RedactingTextStream(sys.stdout)  # type: ignore[assignment]
    if not isinstance(sys.stderr, RedactingTextStream):
        sys.stderr = RedactingTextStream(sys.stderr)  # type: ignore[assignment]


__all__ = [
    "RedactingTextStream",
    "install_output_redaction",
    "redact_sensitive_text",
]

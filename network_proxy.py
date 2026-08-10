#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Process-environment proxy isolation shared by data providers."""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from typing import Iterator


PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)

_PROXY_ENV_LOCK = threading.RLock()


@contextmanager
def scoped_proxy(proxy: str = "") -> Iterator[None]:
    """Use exactly one provider's proxy and restore the process environment.

    A blank value means direct connection. The process environment is global,
    so all providers share one lock while a request temporarily changes it.
    """

    configured = str(proxy or "").strip()
    with _PROXY_ENV_LOCK:
        previous = {key: os.environ.get(key) for key in PROXY_ENV_KEYS}
        try:
            if configured:
                for key in PROXY_ENV_KEYS:
                    os.environ[key] = configured
            else:
                for key in PROXY_ENV_KEYS:
                    os.environ.pop(key, None)
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


__all__ = ["PROXY_ENV_KEYS", "scoped_proxy"]

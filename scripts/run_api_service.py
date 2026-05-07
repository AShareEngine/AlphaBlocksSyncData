#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Start API service."""

from __future__ import annotations

import uvicorn


def main() -> int:
    uvicorn.run("sync_data_system.service.api:app", host="0.0.0.0", port=18080, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

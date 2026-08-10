#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone

from sync_data_system.service.log_time import log_timestamp


def test_log_timestamp_defaults_to_shanghai_time(monkeypatch):
    monkeypatch.delenv("SYNC_LOG_TIMEZONE", raising=False)

    value = log_timestamp(datetime(2026, 8, 10, 8, 31, 50, tzinfo=timezone.utc))

    assert value == "2026-08-10 16:31:50+08:00"


def test_log_timestamp_supports_explicit_timezone(monkeypatch):
    monkeypatch.setenv("SYNC_LOG_TIMEZONE", "UTC")

    value = log_timestamp(datetime(2026, 8, 10, 8, 31, 50, tzinfo=timezone.utc))

    assert value == "2026-08-10 08:31:50+00:00"

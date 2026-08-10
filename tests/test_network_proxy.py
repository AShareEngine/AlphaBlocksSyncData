#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os

from sync_data_system.network_proxy import PROXY_ENV_KEYS, scoped_proxy


def test_scoped_proxy_replaces_all_proxy_variables_and_restores_environment(monkeypatch):
    inherited = {key: f"http://old-{key.lower()}.example" for key in PROXY_ENV_KEYS}
    for key, value in inherited.items():
        monkeypatch.setenv(key, value)

    with scoped_proxy("socks5://127.0.0.1:1080"):
        assert {
            key: os.environ.get(key) for key in PROXY_ENV_KEYS
        } == {key: "socks5://127.0.0.1:1080" for key in PROXY_ENV_KEYS}

    assert {key: os.environ.get(key) for key in PROXY_ENV_KEYS} == inherited


def test_blank_scoped_proxy_forces_direct_connection_and_restores_environment(monkeypatch):
    inherited = "socks5://127.0.0.1:1080"
    for key in PROXY_ENV_KEYS:
        monkeypatch.setenv(key, inherited)

    with scoped_proxy(""):
        assert all(os.environ.get(key) is None for key in PROXY_ENV_KEYS)

    assert all(os.environ.get(key) == inherited for key in PROXY_ENV_KEYS)

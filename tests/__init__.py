#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test package bootstrap."""

from pathlib import Path

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(Path(__file__).resolve().parents[1])

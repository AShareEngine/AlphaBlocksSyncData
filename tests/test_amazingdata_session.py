#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from sync_data_system.providers.amazingdata.provider import (
    AmazingDataSDKConfig,
    AmazingDataSDKSession,
)


def _config() -> AmazingDataSDKConfig:
    return AmazingDataSDKConfig(
        username="demo",
        password="secret",
        host="127.0.0.1",
        port=8600,
        local_path="/tmp/amazingdata-test",
    )


class AmazingDataSDKSessionTest(unittest.TestCase):
    def test_false_login_result_is_accepted_when_session_probe_succeeds(self) -> None:
        module = ModuleType("AmazingData")
        base = Mock()
        base.get_calendar.return_value = [20260727, 20260728]
        module.login = Mock(return_value=False)
        module.logout = Mock()
        module.BaseData = Mock(return_value=base)
        module.InfoData = Mock(return_value=Mock())

        with patch.dict(sys.modules, {"AmazingData": module}):
            session = AmazingDataSDKSession(_config())
            session.ensure_connected()
            self.assertIs(session.base, base)
            session.close()

        base.get_calendar.assert_called_once_with()
        module.logout.assert_called_once_with(username="demo")

    def test_false_login_result_still_fails_when_session_probe_fails(self) -> None:
        module = ModuleType("AmazingData")
        base = Mock()
        base.get_calendar.side_effect = RuntimeError("not authenticated")
        module.login = Mock(return_value=False)
        module.logout = Mock()
        module.BaseData = Mock(return_value=base)
        module.InfoData = Mock(return_value=Mock())

        with patch.dict(sys.modules, {"AmazingData": module}):
            session = AmazingDataSDKSession(_config())
            with self.assertRaisesRegex(RuntimeError, "会话探测失败"):
                session.ensure_connected()

        module.logout.assert_called_once_with(username="demo")


if __name__ == "__main__":
    unittest.main()

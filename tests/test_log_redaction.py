#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import io
import unittest

from sync_data_system.service.log_redaction import (
    RedactingTextStream,
    redact_sensitive_text,
)


class LogRedactionTest(unittest.TestCase):
    def test_redacts_json_plaintext_and_bearer_tokens(self) -> None:
        raw = (
            'logon json: {"Token":"secret-json","name":"demo"} '
            "access_token=secret-text Authorization: Bearer secret-bearer"
        )

        redacted = redact_sensitive_text(raw)

        self.assertNotIn("secret-json", redacted)
        self.assertNotIn("secret-text", redacted)
        self.assertNotIn("secret-bearer", redacted)
        self.assertEqual(redacted.count("[REDACTED]"), 3)

    def test_stream_redacts_before_writing(self) -> None:
        target = io.StringIO()
        stream = RedactingTextStream(target)

        stream.write('{"Token": "secret"}\n')

        self.assertEqual(target.getvalue(), '{"Token": "[REDACTED]"}\n')


if __name__ == "__main__":
    unittest.main()

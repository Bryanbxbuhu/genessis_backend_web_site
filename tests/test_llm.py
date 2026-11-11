import unittest
from types import SimpleNamespace

from helpers.llm import parse_json_completion


def completion(content='{"status":"ok"}', finish_reason="stop", refusal=None):
    return SimpleNamespace(choices=[SimpleNamespace(
        finish_reason=finish_reason,
        message=SimpleNamespace(content=content, refusal=refusal),
    )])


class JsonCompletionTests(unittest.TestCase):
    def test_accepts_complete_json_object(self):
        self.assertEqual(parse_json_completion(completion()), {"status": "ok"})

    def test_rejects_refused_incomplete_or_invalid_content(self):
        cases = [
            completion(finish_reason="length"),
            completion(finish_reason="content_filter"),
            completion(refusal="Cannot comply"),
            completion(content=None),
            completion(content=" "),
            completion(content='{"status":'),
            completion(content="[]"),
            completion(content="null"),
            SimpleNamespace(choices=[]),
        ]
        for response in cases:
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                parse_json_completion(response)

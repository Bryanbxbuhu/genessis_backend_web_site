import unittest

from report_renderer import _safe_http_url


class SafeHttpUrlTests(unittest.TestCase):
    def test_allows_http_and_https(self) -> None:
        self.assertEqual(_safe_http_url("https://example.com/path"), "https://example.com/path")
        self.assertEqual(_safe_http_url("http://example.com"), "http://example.com")

    def test_rejects_script_and_local_schemes(self) -> None:
        self.assertIsNone(_safe_http_url("javascript:alert(1)"))
        self.assertIsNone(_safe_http_url("data:text/html,hello"))
        self.assertIsNone(_safe_http_url("file:///etc/passwd"))


if __name__ == "__main__":
    unittest.main()

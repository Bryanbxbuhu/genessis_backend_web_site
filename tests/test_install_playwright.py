from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from scripts.install_playwright import install_chromium


class ChromiumInstallTests(TestCase):
    @patch("scripts.install_playwright.time.sleep")
    @patch("scripts.install_playwright.subprocess.run")
    def test_success_needs_no_retry(self, run, sleep):
        run.return_value = SimpleNamespace(returncode=0)
        self.assertEqual(install_chromium(with_deps=True), 0)
        self.assertEqual(run.call_args.args[0][-2:], ["--with-deps", "chromium"])
        run.assert_called_once()
        sleep.assert_not_called()

    @patch("scripts.install_playwright.time.sleep")
    @patch("scripts.install_playwright.subprocess.run")
    def test_transient_failure_retries(self, run, sleep):
        run.side_effect = [SimpleNamespace(returncode=100), SimpleNamespace(returncode=0)]
        self.assertEqual(install_chromium(), 0)
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(15)

    @patch("scripts.install_playwright.time.sleep")
    @patch("scripts.install_playwright.subprocess.run")
    def test_exhausted_retries_preserve_failure(self, run, sleep):
        run.return_value = SimpleNamespace(returncode=100)
        self.assertEqual(install_chromium(), 100)
        self.assertEqual(run.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

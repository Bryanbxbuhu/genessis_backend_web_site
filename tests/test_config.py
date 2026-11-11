import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ConfigTests(unittest.TestCase):
    def load_config(self, overrides=None):
        env = os.environ.copy()
        for name in ("OPENAI_API_KEY", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY",
                     "RAPIDAPI_KEY", "RAPID_API_KEY", "OPENAI_MODEL"):
            env.pop(name, None)
        env.update({"CI": "true", "GITHUB_ACTIONS": "true"})
        env.update(overrides or {})
        # Disable local dotenv loading to reproduce a fresh, secret-free CI worker.
        script = (
            "import dotenv; dotenv.load_dotenv = lambda *a, **k: False; "
            "import config, json; "
            "print(json.dumps([config.OPENAI_MODEL, config.OPENAI_API_KEY, config.RAPIDAPI_KEY]))"
        )
        result = subprocess.run([sys.executable, "-c", script], cwd=ROOT, env=env,
                                capture_output=True, text=True, check=True)
        return json.loads(result.stdout)

    def test_ci_can_import_config_without_credentials(self):
        self.assertEqual(self.load_config(), ["gpt-6-luna", "", ""])

    def test_blank_model_uses_default(self):
        self.assertEqual(self.load_config({"OPENAI_MODEL": "  "})[0], "gpt-6-luna")

    def test_model_override_and_rapidapi_alias_are_preserved(self):
        result = self.load_config({"OPENAI_MODEL": " custom-model ", "RAPID_API_KEY": "fixture"})
        self.assertEqual(result, ["custom-model", "", "fixture"])
